package com.nereusstream.delay.runtime;

import com.nereusstream.delay.assessment.DataResetActivationGate;
import com.nereusstream.delay.ownership.ControlTargetRegistrationAuthority;
import com.nereusstream.delay.protocol.ActiveLaneState;
import com.nereusstream.delay.protocol.ApplyShardControlBody;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CancelCommandBody;
import com.nereusstream.delay.protocol.CanonicalLaneTuple;
import com.nereusstream.delay.protocol.CanonicalPayloadCommitProof;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityGrant;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.ChannelResourceIdentity;
import com.nereusstream.delay.protocol.ClaimMaterialization;
import com.nereusstream.delay.protocol.ClaimResultBody;
import com.nereusstream.delay.protocol.CommandBodies;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.CommandType;
import com.nereusstream.delay.protocol.CommitLargeScheduleBody;
import com.nereusstream.delay.protocol.CommittedPayloadDescriptor;
import com.nereusstream.delay.protocol.CompatibleControlSnapshot;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.ControlTargetRef;
import com.nereusstream.delay.protocol.CredentialBindingHead;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryCapabilitySemantic;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.DlqExportResultBody;
import com.nereusstream.delay.protocol.DlqExportState;
import com.nereusstream.delay.protocol.EvidenceCursor;
import com.nereusstream.delay.protocol.InitialRouteControlActivatePayload;
import com.nereusstream.delay.protocol.LaneCircuitState;
import com.nereusstream.delay.protocol.LaneQuotaUsageEntry;
import com.nereusstream.delay.protocol.LaneRecordEnvelope;
import com.nereusstream.delay.protocol.LaneRetirementProgress;
import com.nereusstream.delay.protocol.LaneTerminalGuard;
import com.nereusstream.delay.protocol.LargeScheduleIntent;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.ObjectStoreProfileSemantic;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PayloadCommitProofView;
import com.nereusstream.delay.protocol.PayloadForPublish;
import com.nereusstream.delay.protocol.PayloadProofTrustSet;
import com.nereusstream.delay.protocol.PayloadProofTrustSetControlState;
import com.nereusstream.delay.protocol.PayloadProofTrustSetRef;
import com.nereusstream.delay.protocol.PayloadProofTrustSetSemantic;
import com.nereusstream.delay.protocol.PayloadReference;
import com.nereusstream.delay.protocol.PrepareLargeScheduleBody;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.ProfileAcceptance;
import com.nereusstream.delay.protocol.ProfileBindingActivatePayload;
import com.nereusstream.delay.protocol.ProfileBindingControlState;
import com.nereusstream.delay.protocol.ProfileNewBindingClosePayload;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.ProtocolActivationState;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PublishEvidenceKind;
import com.nereusstream.delay.protocol.PublishOutcomeBody;
import com.nereusstream.delay.protocol.ReadyCertificate;
import com.nereusstream.delay.protocol.ReplayDeadLetterBody;
import com.nereusstream.delay.protocol.RescheduleCommandBody;
import com.nereusstream.delay.protocol.ResolveUncertainBody;
import com.nereusstream.delay.protocol.ResourceDeleteConfirmedBody;
import com.nereusstream.delay.protocol.ResourceKind;
import com.nereusstream.delay.protocol.ResourceRetireIntentBody;
import com.nereusstream.delay.protocol.RetryJitter;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RetryPolicySemantic;
import com.nereusstream.delay.protocol.ScheduleBinding;
import com.nereusstream.delay.protocol.ScheduleCommandBody;
import com.nereusstream.delay.protocol.ShardCapacityEnvelope;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.SloAuthoritativeStartFactory;
import com.nereusstream.delay.protocol.SloObjective;
import com.nereusstream.delay.protocol.SloObjectiveName;
import com.nereusstream.delay.protocol.SloPath;
import com.nereusstream.delay.protocol.SloPopulation;
import com.nereusstream.delay.protocol.SloSampleStart;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationBodyCodec;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TimingCapability;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.protocol.UnsignedInt32;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.IngressFenceState;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.RecoveryCatalogAuthority;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.SloObservationOutboxLimits;
import com.nereusstream.delay.store.SloObservationOutboxStore;
import com.nereusstream.delay.store.ValueEnvelope;
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
import java.util.function.LongSupplier;

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
    private static final int META_PROTOCOL_ACTIVATION_STATE = 14;
    private static final int PAYLOAD_PROOF_CONTROL_VALUE_TYPE = 9;
    private static final int PROFILE_CONTROL_VALUE_TYPE = 10;
    private static final int PROTOCOL_ACTIVATION_VALUE_TYPE = 11;
    /** Legacy compatibility projection; class 1 is reserved for grant identity/version. */
    private static final int META_LEGACY_QUOTA_USAGE = 1;
    /** Registry class 2: canonical aggregate CapacityVector usage. */
    private static final int META_QUOTA_AGGREGATE_USAGE = 2;

    private static final int META_LANE_QUOTA_USAGE = 3;
    private static final int CAPACITY_RESERVE_VALUE_TYPE = 8;
    private static final int CONTROL_RESERVE_NON_OUTCOME_CLASS = 3;
    private static final int CONTROL_RESERVE_RECOVERY_CLASS = 4;
    private static final int CONTROL_RESERVE_EMERGENCY_CLASS = 5;
    private static final int CONTROL_RESERVE_SYSTEM_WRITER_CLASS = 6;
    /** CF-local MESSAGE payload type; current and retired branches share it. */
    private static final int MESSAGE_VALUE_TYPE = 1;

    private static final byte INFLIGHT_CLAIMED_KIND = 1;
    private static final byte INFLIGHT_PUBLISHING_KIND = 2;
    private static final byte INFLIGHT_UNCERTAIN_KIND = 3;
    private static final int DEDUPE_POSITION_VALUE_TYPE = 3;

    private final ShardStore store;
    private final DelayShardConfig config;
    private final PayloadProofTrustSet payloadProofTrustSet;
    private final PayloadProofTrustSetControlCatalog payloadProofTrustSetControlCatalog;
    private final RetryPolicyCatalog retryPolicyCatalog;
    private final ControlTargetRegistrationAuthority controlTargetRegistrationAuthority;
    private final ProfileCatalog profileCatalog;
    /** Optional immutable catalog projection for command-applied SLO Starts. */
    private final SloObjective commandAppliedSloObjective;
    /** Optional ALL_ACCEPTED due-admission objective for source Admission turns. */
    private final SloObjective dueAdmissionSloObjective;
    /** Optional exact H6 barrier for generation-bound source activation. */
    private final DataResetActivationGate dataResetActivationGate;

    private final SloObservationOutboxStore sloObservationOutboxStore;
    private PayloadProofTrustSetControlState payloadProofTrustSetControlState;
    private ProfileBindingControlState profileBindingControlState;
    private ProtocolActivationState protocolActivationState;
    private final ShardCapacityEnvelope capacityEnvelope;
    private final ScheduleResolver scheduleResolver;
    /** Single-writer scratch; consumed by the same apply turn before the batch is written. */
    private ScheduleResolver.ResolvedSchedule lastResolvedSchedule;
    /** Message identity for the single-writer Schedule projection scratch. */
    private DelayMessageId lastResolvedScheduleMessageId;

    private ScheduleResolver.ResolvedPrepare lastResolvedPrepare;
    private SourcePosition lastAppliedSourcePosition;
    private long closedIngressDeadlineThrough;
    private long mutationSequence;
    private long claimSequence;
    private ShardQuota quota;
    private LaneQuotaUsageProjection laneQuotaUsage;
    private OutcomeReserveUsage outcomeReserve;
    private CapacityVector outcomeReserveVector;
    private final Map<Integer, CapacityVector> controlReserveUsage = new HashMap<>();

    public DelayShard(final ShardStore store, final DelayShardConfig config) {
        this(store, config, null, null, null, null);
    }

    public DelayShard(
            final ShardStore store, final DelayShardConfig config, final PayloadProofTrustSet payloadProofTrustSet) {
        this(store, config, payloadProofTrustSet, null, null, null);
    }

    /**
     * Opens a shard with an immutable capacity envelope supplied by the
     * placement/activation authority. The envelope is persisted before any
     * command can charge its outcome grant; a different grant or digest can
     * therefore never reuse this DB's usage projection.
     */
    public DelayShard(
            final ShardStore store,
            final DelayShardConfig config,
            final PayloadProofTrustSet payloadProofTrustSet,
            final ShardCapacityEnvelope capacityEnvelope) {
        this(store, config, payloadProofTrustSet, capacityEnvelope, null, null);
    }

    /**
     * Opens a shard with an explicit source-position-pinned Schedule
     * resolver. The resolver is optional for legacy commands; a Registry
     * Schedule/Prepare body without one fails closed with
     * {@link StableCode#ROUTE_SNAPSHOT_UNAVAILABLE}.
     */
    public DelayShard(
            final ShardStore store,
            final DelayShardConfig config,
            final PayloadProofTrustSet payloadProofTrustSet,
            final ShardCapacityEnvelope capacityEnvelope,
            final ScheduleResolver scheduleResolver) {
        this(store, config, payloadProofTrustSet, capacityEnvelope, scheduleResolver, null);
    }

    /**
     * Opens a shard with the local source-ordered trust-set marker projection.
     * The catalog is required before kind-12/kind-13 controls can mutate the
     * projection; missing or mismatched semantic bytes fail closed.
     */
    public DelayShard(
            final ShardStore store,
            final DelayShardConfig config,
            final PayloadProofTrustSet payloadProofTrustSet,
            final ShardCapacityEnvelope capacityEnvelope,
            final ScheduleResolver scheduleResolver,
            final PayloadProofTrustSetControlCatalog payloadProofTrustSetControlCatalog) {
        this(
                store,
                config,
                payloadProofTrustSet,
                capacityEnvelope,
                scheduleResolver,
                payloadProofTrustSetControlCatalog,
                null);
    }

    /**
     * Opens a shard with an optional source-position-pinned Retry Policy
     * catalog. A supplied catalog turns on fail-closed semantic lookup for
     * Schedule/Prepare; older constructors retain compatibility behavior.
     */
    public DelayShard(
            final ShardStore store,
            final DelayShardConfig config,
            final PayloadProofTrustSet payloadProofTrustSet,
            final ShardCapacityEnvelope capacityEnvelope,
            final ScheduleResolver scheduleResolver,
            final PayloadProofTrustSetControlCatalog payloadProofTrustSetControlCatalog,
            final RetryPolicyCatalog retryPolicyCatalog) {
        this(
                store,
                config,
                payloadProofTrustSet,
                capacityEnvelope,
                scheduleResolver,
                payloadProofTrustSetControlCatalog,
                retryPolicyCatalog,
                null);
    }

    /**
     * Opens a shard with an optional exact Control target-registration authority.
     * When supplied, source-ordered Control markers are rejected before their
     * local handlers run unless the immutable Prepared target registration is
     * present and the marker bytes match it exactly.
     */
    public DelayShard(
            final ShardStore store,
            final DelayShardConfig config,
            final PayloadProofTrustSet payloadProofTrustSet,
            final ShardCapacityEnvelope capacityEnvelope,
            final ScheduleResolver scheduleResolver,
            final PayloadProofTrustSetControlCatalog payloadProofTrustSetControlCatalog,
            final RetryPolicyCatalog retryPolicyCatalog,
            final ControlTargetRegistrationAuthority controlTargetRegistrationAuthority) {
        this(
                store,
                config,
                payloadProofTrustSet,
                capacityEnvelope,
                scheduleResolver,
                payloadProofTrustSetControlCatalog,
                retryPolicyCatalog,
                controlTargetRegistrationAuthority,
                null);
    }

    /**
     * Opens a shard with an optional exact Profile semantic catalog. When
     * supplied, the catalog decorates a raw Schedule resolver with the
     * exact Profile/Capability and credential-Head gate, and Publish
     * Admission validates descriptor timing against the same semantics;
     * callers that already pass a {@link ProfileCatalogScheduleResolver}
     * are not wrapped twice. Without a catalog, only ordinary managed
     * {@code actionAt=deliverAt} is accepted by the local compatibility path.
     */
    public DelayShard(
            final ShardStore store,
            final DelayShardConfig config,
            final PayloadProofTrustSet payloadProofTrustSet,
            final ShardCapacityEnvelope capacityEnvelope,
            final ScheduleResolver scheduleResolver,
            final PayloadProofTrustSetControlCatalog payloadProofTrustSetControlCatalog,
            final RetryPolicyCatalog retryPolicyCatalog,
            final ControlTargetRegistrationAuthority controlTargetRegistrationAuthority,
            final ProfileCatalog profileCatalog) {
        this(
                store,
                config,
                payloadProofTrustSet,
                capacityEnvelope,
                scheduleResolver,
                payloadProofTrustSetControlCatalog,
                retryPolicyCatalog,
                controlTargetRegistrationAuthority,
                profileCatalog,
                null,
                null,
                null);
    }

    /**
     * Opens a shard with an optional immutable command-applied SLO objective.
     * When present, every client-command source turn materializes the exact
     * {@code COMMAND_APPLIED_LATENCY} Start in the same synchronous RocksDB
     * batch as the command result and applied Source Position. The objective
     * must come from the caller's authenticated immutable catalog projection;
     * this constructor does not make that catalog or its writer authority
     * authoritative.
     */
    public DelayShard(
            final ShardStore store,
            final DelayShardConfig config,
            final PayloadProofTrustSet payloadProofTrustSet,
            final ShardCapacityEnvelope capacityEnvelope,
            final ScheduleResolver scheduleResolver,
            final PayloadProofTrustSetControlCatalog payloadProofTrustSetControlCatalog,
            final RetryPolicyCatalog retryPolicyCatalog,
            final ControlTargetRegistrationAuthority controlTargetRegistrationAuthority,
            final ProfileCatalog profileCatalog,
            final SloObjective commandAppliedSloObjective) {
        this(
                store,
                config,
                payloadProofTrustSet,
                capacityEnvelope,
                scheduleResolver,
                payloadProofTrustSetControlCatalog,
                retryPolicyCatalog,
                controlTargetRegistrationAuthority,
                profileCatalog,
                commandAppliedSloObjective,
                null,
                null);
    }

    /**
     * Full constructor with the per-shard SLO outbox capacity envelope.
     * Reaching the envelope fails the source turn before the business batch
     * can commit; callers must not drop a Start or shrink the denominator.
     */
    public DelayShard(
            final ShardStore store,
            final DelayShardConfig config,
            final PayloadProofTrustSet payloadProofTrustSet,
            final ShardCapacityEnvelope capacityEnvelope,
            final ScheduleResolver scheduleResolver,
            final PayloadProofTrustSetControlCatalog payloadProofTrustSetControlCatalog,
            final RetryPolicyCatalog retryPolicyCatalog,
            final ControlTargetRegistrationAuthority controlTargetRegistrationAuthority,
            final ProfileCatalog profileCatalog,
            final SloObjective commandAppliedSloObjective,
            final SloObservationOutboxLimits sloObservationOutboxLimits) {
        this(
                store,
                config,
                payloadProofTrustSet,
                capacityEnvelope,
                scheduleResolver,
                payloadProofTrustSetControlCatalog,
                retryPolicyCatalog,
                controlTargetRegistrationAuthority,
                profileCatalog,
                commandAppliedSloObjective,
                null,
                sloObservationOutboxLimits);
    }

    /**
     * Full SLO objective wiring for the local source-ordered Admission seam.
     * The due objective is restricted to the ALL_ACCEPTED population: a
     * HEALTHY due sample requires a separate full-interval predicate proof and
     * must not be inferred from an Admission body.
     */
    public DelayShard(
            final ShardStore store,
            final DelayShardConfig config,
            final PayloadProofTrustSet payloadProofTrustSet,
            final ShardCapacityEnvelope capacityEnvelope,
            final ScheduleResolver scheduleResolver,
            final PayloadProofTrustSetControlCatalog payloadProofTrustSetControlCatalog,
            final RetryPolicyCatalog retryPolicyCatalog,
            final ControlTargetRegistrationAuthority controlTargetRegistrationAuthority,
            final ProfileCatalog profileCatalog,
            final SloObjective commandAppliedSloObjective,
            final SloObjective dueAdmissionSloObjective,
            final SloObservationOutboxLimits sloObservationOutboxLimits) {
        this(
                store,
                config,
                payloadProofTrustSet,
                capacityEnvelope,
                scheduleResolver,
                payloadProofTrustSetControlCatalog,
                retryPolicyCatalog,
                controlTargetRegistrationAuthority,
                profileCatalog,
                commandAppliedSloObjective,
                dueAdmissionSloObjective,
                sloObservationOutboxLimits,
                null);
    }

    /** Full constructor with an optional signed H6 DataResetManifest gate. */
    public DelayShard(
            final ShardStore store,
            final DelayShardConfig config,
            final PayloadProofTrustSet payloadProofTrustSet,
            final ShardCapacityEnvelope capacityEnvelope,
            final ScheduleResolver scheduleResolver,
            final PayloadProofTrustSetControlCatalog payloadProofTrustSetControlCatalog,
            final RetryPolicyCatalog retryPolicyCatalog,
            final ControlTargetRegistrationAuthority controlTargetRegistrationAuthority,
            final ProfileCatalog profileCatalog,
            final SloObjective commandAppliedSloObjective,
            final SloObjective dueAdmissionSloObjective,
            final SloObservationOutboxLimits sloObservationOutboxLimits,
            final DataResetActivationGate dataResetActivationGate) {
        this.store = Objects.requireNonNull(store, "store");
        this.config = Objects.requireNonNull(config, "config");
        this.payloadProofTrustSet = payloadProofTrustSet;
        this.payloadProofTrustSetControlCatalog = payloadProofTrustSetControlCatalog;
        this.retryPolicyCatalog = retryPolicyCatalog;
        this.controlTargetRegistrationAuthority = controlTargetRegistrationAuthority;
        this.profileCatalog = profileCatalog;
        if (commandAppliedSloObjective != null
                && commandAppliedSloObjective.name() != SloObjectiveName.COMMAND_APPLIED_LATENCY) {
            throw new IllegalArgumentException("command-applied SLO objective has the wrong name");
        }
        if (dueAdmissionSloObjective != null
                && (dueAdmissionSloObjective.name() != SloObjectiveName.DUE_ADMISSION_LAG
                        || dueAdmissionSloObjective.population() != SloPopulation.ALL_ACCEPTED)) {
            throw new IllegalArgumentException("due-admission objective must be ALL_ACCEPTED DUE_ADMISSION_LAG");
        }
        this.commandAppliedSloObjective = commandAppliedSloObjective;
        this.dueAdmissionSloObjective = dueAdmissionSloObjective;
        this.dataResetActivationGate = dataResetActivationGate;
        this.sloObservationOutboxStore = commandAppliedSloObjective == null
                        && dueAdmissionSloObjective == null
                        && sloObservationOutboxLimits == null
                ? null
                : new SloObservationOutboxStore(store, sloObservationOutboxLimits);
        this.capacityEnvelope = capacityEnvelope;
        this.scheduleResolver = bindProfileCatalogResolver(scheduleResolver, profileCatalog);
        final var sourceValue = store.getValue(ColumnFamily.META, KeyCodec.metaFixed(META_APPLIED_SOURCE_POSITION), 1);
        final byte[] source = sourceValue == null ? null : sourceValue.payload();
        lastAppliedSourcePosition = source == null ? null : SourcePositionCodec.decode(source);
        if (lastAppliedSourcePosition != null && !store.shardId().equals(lastAppliedSourcePosition.shardId())) {
            throw new IllegalStateException("persisted applied source position belongs to another shard");
        }
        final var closedDeadline =
                store.getValue(ColumnFamily.META, KeyCodec.metaFixed(META_CLOSED_INGRESS_DEADLINE), 1);
        closedIngressDeadlineThrough = closedDeadline == null
                ? IngressFenceState.OPEN
                : IngressFenceState.decode(closedDeadline.payload()).closedThroughEpochMs();
        final var sequence = store.getValue(ColumnFamily.META, KeyCodec.metaFixed(META_MUTATION_SEQUENCE), 1);
        mutationSequence = sequence == null ? 0 : readUnsignedSequence(sequence.payload());
        final var claimSequenceValue = store.getValue(ColumnFamily.META, KeyCodec.metaFixed(META_CLAIM_SEQUENCE), 1);
        claimSequence = claimSequenceValue == null ? 0 : readUnsignedSequence(claimSequenceValue.payload());
        final var payloadProofControlValue = store.getValue(
                ColumnFamily.META,
                KeyCodec.metaFixed(META_PAYLOAD_PROOF_CONTROL_STATE),
                PAYLOAD_PROOF_CONTROL_VALUE_TYPE);
        payloadProofTrustSetControlState = payloadProofControlValue == null
                ? PayloadProofTrustSetControlState.empty()
                : PayloadProofTrustSetControlState.decode(payloadProofControlValue.payload());
        final var profileControlValue = store.getValue(
                ColumnFamily.META, KeyCodec.metaFixed(META_PROFILE_CONTROL_STATE), PROFILE_CONTROL_VALUE_TYPE);
        profileBindingControlState = profileControlValue == null
                ? ProfileBindingControlState.empty()
                : ProfileBindingControlState.decode(profileControlValue.payload());
        final var protocolActivationValue = store.getValue(
                ColumnFamily.META, KeyCodec.metaFixed(META_PROTOCOL_ACTIVATION_STATE), PROTOCOL_ACTIVATION_VALUE_TYPE);
        protocolActivationState = protocolActivationValue == null
                ? null
                : ProtocolActivationState.decode(protocolActivationValue.payload());
        if (protocolActivationState != null
                && !store.shardId().equals(protocolActivationState.shard().shardId())) {
            throw new IllegalStateException("persisted protocol activation state belongs to another shard");
        }
        validateControlStateSourcePositions();
        final var quotaValue = store.getValue(ColumnFamily.META, KeyCodec.metaQuota(META_LEGACY_QUOTA_USAGE), 7);
        final ShardQuota persistedQuota = quotaValue == null ? null : ShardQuota.decode(quotaValue.payload());
        quota = persistedQuota == null ? ShardQuota.empty() : persistedQuota;
        final var laneQuotaValue = store.getValue(ColumnFamily.META, KeyCodec.metaQuota(META_LANE_QUOTA_USAGE), 7);
        final LaneQuotaUsageProjection persistedLaneQuota =
                laneQuotaValue == null ? null : LaneQuotaUsageProjection.decode(laneQuotaValue.payload());
        rejectUnsupportedQuotaProjections();
        final long rebuildRevision = persistedQuota == null
                ? persistedLaneQuota == null ? 0 : maxLaneQuotaRevision(persistedLaneQuota)
                : persistedQuota.usageRevision();
        final LaneQuotaUsageProjection rebuiltLaneQuota = rebuildLaneQuotaUsage(rebuildRevision);
        final ShardQuota rebuiltQuota = rebuildShardQuota(rebuiltLaneQuota, rebuildRevision);
        if (persistedQuota == null) {
            // Legacy stores may have the durable records but no aggregate
            // projection yet. Reuse the rebuilt counts in memory; the next
            // source-ordered mutation persists the canonical value.
            quota = rebuiltQuota;
        } else if (!sameQuotaUsage(persistedQuota, rebuiltQuota)) {
            throw new IllegalStateException("persisted shard quota disagrees with runtime state");
        }
        laneQuotaUsage = persistedLaneQuota == null ? rebuiltLaneQuota : persistedLaneQuota;
        if (!Arrays.equals(laneQuotaUsage.canonicalBytes(), rebuiltLaneQuota.canonicalBytes())) {
            throw new IllegalStateException("persisted per-Lane quota projection disagrees with runtime state");
        }
        validateTypedActiveLaneQuotaProjection(persistedLaneQuota);
        final var aggregateQuotaValue =
                store.getValue(ColumnFamily.META, KeyCodec.metaQuota(META_QUOTA_AGGREGATE_USAGE), 7);
        CapacityVector persistedQuotaAggregate = null;
        OutcomeReserveUsage legacyPersistedOutcomeReserve = null;
        if (aggregateQuotaValue != null) {
            try {
                persistedQuotaAggregate = CapacityVector.decode(aggregateQuotaValue.payload());
            } catch (IllegalArgumentException notAnAggregateVector) {
                // Stores written before the Registry class-2 projection was
                // closed carried the scalar OutcomeReserveUsage here. Keep
                // it readable for one-way source-ordered migration, but do
                // not treat it as the class-2 value.
                legacyPersistedOutcomeReserve = OutcomeReserveUsage.decode(aggregateQuotaValue.payload());
            }
        }
        final OutcomeReserveUsage rebuiltOutcomeReserve = rebuildOutcomeReserveUsage();
        if (legacyPersistedOutcomeReserve != null && !legacyPersistedOutcomeReserve.equals(rebuiltOutcomeReserve)) {
            throw new IllegalStateException("persisted outcome reserve disagrees with runtime state");
        }
        outcomeReserve = rebuiltOutcomeReserve;
        if (outcomeReserve.records() > config.maxOutcomeReserveRecords()
                || outcomeReserve.bytes() > config.maxOutcomeReserveBytes()) {
            throw new IllegalStateException("persisted outcome reserve exceeds the active shard grant");
        }
        outcomeReserveVector = loadCapacityEnvelopeState(capacityEnvelope);
        final CapacityVector rebuiltOutcomeReserveVector = rebuildOutcomeReserveVector();
        if (capacityEnvelope == null) {
            // Compatibility shards do not persist a grant-bound vector, but
            // their open attempt ledgers still carry the canonical Admission
            // charge. Rebuild that local projection as well; otherwise a
            // restart would forget the vector and a later release could
            // underflow even though the scalar reserve was restored.
            outcomeReserveVector = rebuiltOutcomeReserveVector;
        } else if (!outcomeReserveVector.equals(rebuiltOutcomeReserveVector)) {
            throw new IllegalStateException("persisted outcome reserve vector disagrees with runtime state");
        }
        if (persistedQuotaAggregate != null && !outcomeReserve.equals(outcomeReserveUsage(persistedQuotaAggregate))) {
            throw new IllegalStateException("persisted outcome reserve disagrees with runtime state");
        }
        final CapacityVector rebuiltQuotaAggregate = aggregateQuotaUsage(rebuiltLaneQuota, rebuiltOutcomeReserveVector);
        if (persistedQuotaAggregate != null && !persistedQuotaAggregate.equals(rebuiltQuotaAggregate)) {
            throw new IllegalStateException("persisted quota aggregate disagrees with runtime state");
        }
        loadControlReserveUsage(capacityEnvelope);
        if (capacityEnvelope != null && !outcomeReserve.equals(outcomeReserveUsage(outcomeReserveVector))) {
            throw new IllegalStateException("persisted outcome reserve projections disagree");
        }
        validateRuntimeObligationIndexes();
        // Bind a first-seen capacity envelope only after every persisted
        // reserve, quota and obligation projection has passed validation.
        // A failed open must not leave an identity marker behind that makes a
        // later repaired activation appear to be envelope drift.
        persistCapacityEnvelopeBindingIfAbsent(capacityEnvelope);
    }

    private static ScheduleResolver bindProfileCatalogResolver(
            final ScheduleResolver resolver, final ProfileCatalog catalog) {
        if (resolver == null) {
            return null;
        }
        if (resolver instanceof ProfileCatalogScheduleResolver decorated) {
            if (catalog == null) {
                throw new IllegalArgumentException(
                        "Profile catalog Schedule resolver requires the shard Profile catalog");
            }
            decorated.requireProfileCatalog(catalog);
            return decorated;
        }
        return catalog == null ? resolver : new ProfileCatalogScheduleResolver(resolver, catalog);
    }

    /**
     * Fences the typed ACTIVE Lane projection against the Registry class-3
     * quota map before activation. The typed state's field 14 is a durable
     * mirror of the exact (Lane ID, incarnation) entry; opening a typed Lane
     * without that map or with a different usage vector would make recovery
     * depend on an unproven projection, so the shard fails closed.
     */
    private void validateTypedActiveLaneQuotaProjection(final LaneQuotaUsageProjection persistedLaneQuota) {
        final long configuredLimit = Math.max(config.maxPendingMessages(), config.maxLanes());
        final int limit = boundedLimitPlusOne(configuredLimit);
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> laneEntries =
                store.scan(ColumnFamily.META, new byte[] {2, 1}, new byte[] {3, 1}, limit);
        if (laneEntries.size() >= limit && configuredLimit < Integer.MAX_VALUE) {
            throw new IllegalStateException("typed Lane quota validation exceeded configured bound");
        }
        for (var entry : laneEntries) {
            final byte[] key = entry.key();
            if (key.length != 2 + DestinationLaneId.LENGTH || key[0] != 2 || key[1] != 1) {
                throw new IllegalStateException("invalid Lane key during typed quota validation");
            }
            final DestinationLaneId laneId = new DestinationLaneId(Arrays.copyOfRange(key, 2, key.length));
            final LaneValue value =
                    decodeLaneValue(ValueEnvelope.decode(entry.value(), 2).payload());
            if (!value.isActive() || value.typedActiveState() == null) {
                continue;
            }
            if (persistedLaneQuota == null) {
                throw new IllegalStateException("typed ACTIVE Lane is missing the class-3 quota map");
            }
            final ActiveLaneState state = value.typedActiveState();
            if (!state.laneId().equals(laneId)) {
                throw new IllegalStateException("typed Lane key/value identity mismatch during quota validation");
            }
            final PublishAdmissionBody.ChargeVector projectedUsage =
                    persistedLaneQuota.usageFor(state.laneId(), state.laneIncarnation());
            if (!state.laneUsage().equals(projectedUsage)) {
                throw new IllegalStateException("typed Lane usage disagrees with the class-3 quota map");
            }
            validateTypedReadyKey(state);
        }
    }

    /** Ensures typed READY metadata is the exact key derived from its Lane state. */
    private static void validateTypedReadyKey(final ActiveLaneState state) {
        final byte[] encodedReadyKey = state.encodedReadyKey();
        if (encodedReadyKey == null) {
            return;
        }
        if (state.admissionGate() != AdmissionGate.OPEN
                || state.runtimeReadiness() != RuntimeReadiness.READY
                || state.nextEligibleAtEpochMs() == null) {
            throw new IllegalStateException("typed READY key is present for a non-schedulable Lane state");
        }
        final byte[] expected =
                KeyCodec.timelineReady(state.nextEligibleAtEpochMs(), state.laneId(), state.laneVersion());
        if (!Arrays.equals(encodedReadyKey, expected)) {
            throw new IllegalStateException("typed READY key disagrees with Lane state");
        }
        final ReadyKey decoded = decodeReadyKey(encodedReadyKey);
        if (!decoded.laneId().equals(state.laneId())
                || decoded.laneVersion() != state.laneVersion()
                || decoded.nextEligibleAtEpochMs() != state.nextEligibleAtEpochMs()) {
            throw new IllegalStateException("typed READY key fields disagree with Lane state");
        }
    }

    /**
     * registers quota classes 4 and 5, but their value schemas are not
     * frozen yet. Do not silently treat a persisted retained/object or
     * grandfathered-transfer projection as empty; activation must stop until
     * a Registry revision supplies the decoder and accounting rules.
     */
    private void rejectUnsupportedQuotaProjections() {
        for (int quotaClass = 4; quotaClass <= 5; quotaClass++) {
            if (store.get(ColumnFamily.META, KeyCodec.metaQuota(quotaClass)) != null) {
                throw new IllegalStateException("meta/QUOTA quotaClass=" + quotaClass
                        + " value schema is not implemented; activation is fail-closed");
            }
        }
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
        lastResolvedScheduleMessageId = null;
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
                if (prior != null
                        && prior.protocolTuple().equals(command.protocolTuple())
                        && Bytes.constantTimeEquals(prior.commandHash(), command.commandHash())) {
                    final PositionAudit audit = readPositionAudit(sourcePosition);
                    if (audit == null
                            || audit.commandId() == null
                            || !audit.commandId().equals(command.commandId())) {
                        throw new IllegalStateException("duplicate command source position has conflicting evidence");
                    }
                    if (commandRetryWindowExpired(command, sourcePosition)) {
                        // A later physical duplicate can be outside the
                        // command's retry window even though the first
                        // logical result remains valid. Keep that result
                        // immutable, but replay the exact position-level
                        // rejection rather than exposing the first result for
                        // this physical record.
                        repairCommandAppliedStartAfterExistingApply(sourcePosition);
                        return rejected(StableCode.COMMAND_RETRY_WINDOW_EXPIRED, sourcePosition, -1, 0, null);
                    }
                    if (!Bytes.constantTimeEquals(
                            prior.result().appliedSourcePosition(), sourcePosition.canonicalBytes())) {
                        if (!Bytes.constantTimeEquals(
                                lastAppliedSourcePosition.canonicalBytes(), sourcePosition.canonicalBytes())) {
                            throw new IllegalStateException(
                                    "duplicate command position has conflicting source identity");
                        }
                    }
                    repairCommandAppliedStartAfterExistingApply(sourcePosition);
                    return prior.result();
                }
                final CommandId positionCommandId = readPositionAuditCommandId(sourcePosition);
                if (positionCommandId != null && positionCommandId.equals(command.commandId())) {
                    if (prior != null) {
                        // The position audit is the evidence that this exact
                        // physical record already produced the conflict. Do
                        // not append another audit or mutate the first command
                        // identity while replaying after a lost source ACK.
                        repairCommandAppliedStartAfterExistingApply(sourcePosition);
                        return rejected(StableCode.COMMAND_ID_CONFLICT, sourcePosition, -1, 0, null);
                    }
                    if (commandRetryWindowExpired(command, sourcePosition)) {
                        // A fence rejection deliberately has no logical
                        // COMMAND/RESULT record. Its POSITION audit is still
                        // sufficient to make the exact source record replay
                        // idempotent after the RocksDB batch was acknowledged.
                        repairCommandAppliedStartAfterExistingApply(sourcePosition);
                        return rejected(StableCode.COMMAND_RETRY_WINDOW_EXPIRED, sourcePosition, -1, 0, null);
                    }
                }
                throw new IllegalStateException("duplicate source position without matching command evidence");
            }
        }
        if (commandRetryWindowExpired(command, sourcePosition)) {
            return persistRejectedPositionOnly(command, sourcePosition, StableCode.COMMAND_RETRY_WINDOW_EXPIRED);
        }
        final CommandDedupeRecord prior = readCommandDedupe(command.commandId());
        if (prior != null) {
            if (!prior.protocolTuple().equals(command.protocolTuple())
                    || !Bytes.constantTimeEquals(prior.commandHash(), command.commandHash())) {
                final CommandResult conflict = rejected(StableCode.COMMAND_ID_CONFLICT, sourcePosition, -1, 0, null);
                persistCommandOnly(command, sourcePosition);
                return conflict;
            }
            persistPositionOnly(command, sourcePosition);
            return prior.result();
        }
        try {
            validateFirstSeenCommandIdentity(command, sourcePosition);
        } catch (ArithmeticException | IllegalArgumentException exception) {
            return persistRejected(command, sourcePosition, StableCode.INVALID_COMMAND);
        }
        if (sourcePosition.brokerPersistenceTimeEpochMs() > command.retryUntilEpochMs()) {
            return persistRejected(command, sourcePosition, StableCode.COMMAND_RETRY_WINDOW_EXPIRED);
        }
        final StableCode protocolCode = validateCommandProtocolTuple(command);
        if (protocolCode != null) {
            return persistRejected(command, sourcePosition, protocolCode);
        }

        if (command.type() == com.nereusstream.delay.protocol.CommandType.PREPARE_LARGE_SCHEDULE
                || command.type() == com.nereusstream.delay.protocol.CommandType.COMMIT_LARGE_SCHEDULE) {
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
        } catch (CommandResolutionException exception) {
            return persistRejected(command, sourcePosition, exception.stableCode());
        } catch (ArithmeticException | IllegalArgumentException exception) {
            return persistRejected(command, sourcePosition, StableCode.INVALID_COMMAND);
        }
        persistResultAndPosition(command, sourcePosition, result, nextMessage(command, sourcePosition, result));
        return result;
    }

    private CapacityVector loadCapacityEnvelopeState(final ShardCapacityEnvelope envelope) {
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> bindingEntries = scanControlReserveClass(1);
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> usageEntries = scanControlReserveClass(2);
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> nonOutcomeEntries = scanControlReserveClass(3);
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> recoveryEntries = scanControlReserveClass(4);
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> emergencyEntries = scanControlReserveClass(5);
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> systemWriterEntries = scanControlReserveClass(6);
        if (envelope == null) {
            if (!bindingEntries.isEmpty()
                    || !usageEntries.isEmpty()
                    || !nonOutcomeEntries.isEmpty()
                    || !recoveryEntries.isEmpty()
                    || !emergencyEntries.isEmpty()
                    || !systemWriterEntries.isEmpty()) {
                throw new IllegalStateException("capacity envelope is required for persisted control reserve state");
            }
            return CapacityVector.empty();
        }
        final byte[] bindingKey =
                KeyCodec.metaControlReserve(1, envelope.outcomeReserve().grantId());
        if (bindingEntries.size() > 1
                || !bindingEntries.isEmpty()
                        && !Arrays.equals(bindingEntries.get(0).key(), bindingKey)) {
            throw new IllegalStateException("persisted control reserve grant identity does not match envelope");
        }
        final byte[] usageKey =
                KeyCodec.metaControlReserve(2, envelope.outcomeReserve().grantId());
        if (usageEntries.size() > 1
                || !usageEntries.isEmpty() && !Arrays.equals(usageEntries.get(0).key(), usageKey)) {
            throw new IllegalStateException("persisted outcome reserve key does not match envelope grant");
        }
        final ValueEnvelope.Decoded persistedBinding =
                store.getValue(ColumnFamily.META, bindingKey, CAPACITY_RESERVE_VALUE_TYPE);
        if (persistedBinding == null) {
            if (!usageEntries.isEmpty()) {
                throw new IllegalStateException("persisted outcome reserve has no envelope identity");
            }
        } else if (!envelope.equals(ShardCapacityEnvelope.decode(persistedBinding.payload()))) {
            throw new IllegalStateException("persisted capacity envelope identity differs from active envelope");
        }
        final CapacityVector nonOutcomeUsage = reserveProjectionUsage(
                nonOutcomeEntries,
                CONTROL_RESERVE_NON_OUTCOME_CLASS,
                envelope.nonOutcomeControl(),
                "non-outcome control");
        final CapacityVector systemWriterUsage = reserveProjectionUsage(
                systemWriterEntries,
                CONTROL_RESERVE_SYSTEM_WRITER_CLASS,
                envelope.nonOutcomeControl(),
                "system-writer");
        validateControlReservePartition(nonOutcomeUsage, false, "non-outcome control");
        validateControlReservePartition(systemWriterUsage, true, "system-writer");
        ensureNonOutcomeProjectionFits(envelope.nonOutcomeControl(), nonOutcomeUsage, systemWriterUsage);
        reserveProjectionUsage(
                recoveryEntries, CONTROL_RESERVE_RECOVERY_CLASS, envelope.recoveryWorking(), "recovery working");
        reserveProjectionUsage(
                emergencyEntries, CONTROL_RESERVE_EMERGENCY_CLASS, envelope.emergencyHeadroom(), "emergency headroom");
        final ValueEnvelope.Decoded persistedUsage =
                store.getValue(ColumnFamily.META, usageKey, CAPACITY_RESERVE_VALUE_TYPE);
        final CapacityVector usage =
                persistedUsage == null ? CapacityVector.empty() : CapacityVector.decode(persistedUsage.payload());
        if (!envelope.outcomeReserve().vector().covers(usage)) {
            throw new IllegalStateException("persisted outcome reserve exceeds immutable capacity grant");
        }
        return usage;
    }

    private void persistCapacityEnvelopeBindingIfAbsent(final ShardCapacityEnvelope envelope) {
        if (envelope == null) {
            return;
        }
        final byte[] bindingKey =
                KeyCodec.metaControlReserve(1, envelope.outcomeReserve().grantId());
        if (store.getValue(ColumnFamily.META, bindingKey, CAPACITY_RESERVE_VALUE_TYPE) == null) {
            store.write(batch -> batch.putValue(
                    ColumnFamily.META, CAPACITY_RESERVE_VALUE_TYPE, bindingKey, envelope.canonicalBytes()));
        }
    }

    private void loadControlReserveUsage(final ShardCapacityEnvelope envelope) {
        if (envelope == null) {
            return;
        }
        final CapacityVector nonOutcomeUsage = reserveProjectionUsage(
                scanControlReserveClass(CONTROL_RESERVE_NON_OUTCOME_CLASS),
                CONTROL_RESERVE_NON_OUTCOME_CLASS,
                envelope.nonOutcomeControl(),
                "non-outcome control");
        final CapacityVector systemWriterUsage = reserveProjectionUsage(
                scanControlReserveClass(CONTROL_RESERVE_SYSTEM_WRITER_CLASS),
                CONTROL_RESERVE_SYSTEM_WRITER_CLASS,
                envelope.nonOutcomeControl(),
                "system-writer");
        validateControlReservePartition(nonOutcomeUsage, false, "non-outcome control");
        validateControlReservePartition(systemWriterUsage, true, "system-writer");
        ensureNonOutcomeProjectionFits(envelope.nonOutcomeControl(), nonOutcomeUsage, systemWriterUsage);
        controlReserveUsage.put(CONTROL_RESERVE_NON_OUTCOME_CLASS, nonOutcomeUsage);
        controlReserveUsage.put(
                CONTROL_RESERVE_RECOVERY_CLASS,
                reserveProjectionUsage(
                        scanControlReserveClass(CONTROL_RESERVE_RECOVERY_CLASS),
                        CONTROL_RESERVE_RECOVERY_CLASS,
                        envelope.recoveryWorking(),
                        "recovery working"));
        controlReserveUsage.put(
                CONTROL_RESERVE_EMERGENCY_CLASS,
                reserveProjectionUsage(
                        scanControlReserveClass(CONTROL_RESERVE_EMERGENCY_CLASS),
                        CONTROL_RESERVE_EMERGENCY_CLASS,
                        envelope.emergencyHeadroom(),
                        "emergency headroom"));
        controlReserveUsage.put(CONTROL_RESERVE_SYSTEM_WRITER_CLASS, systemWriterUsage);
    }

    private CapacityVector reserveProjectionUsage(
            final List<com.nereusstream.delay.store.ShardStore.KeyValue> entries,
            final int reserveClass,
            final CapacityGrant grant,
            final String name) {
        if (entries.size() > 1) {
            throw new IllegalStateException("multiple " + name + " reserve projections exist");
        }
        if (entries.isEmpty()) {
            return CapacityVector.empty();
        }
        final byte[] expectedKey = KeyCodec.metaControlReserve(reserveClass, grant.grantId());
        if (!Arrays.equals(entries.get(0).key(), expectedKey)) {
            throw new IllegalStateException(name + " reserve grant identity does not match envelope");
        }
        final CapacityVector usage =
                CapacityVector.decode(ValueEnvelope.decode(entries.get(0).value(), CAPACITY_RESERVE_VALUE_TYPE)
                        .payload());
        if (!grant.vector().covers(usage)) {
            throw new IllegalStateException(name + " reserve usage exceeds immutable capacity grant");
        }
        return usage;
    }

    private List<com.nereusstream.delay.store.ShardStore.KeyValue> scanControlReserveClass(final int reserveClass) {
        final byte[] lower = new byte[] {6, 1, (byte) reserveClass};
        final byte[] upper = new byte[] {6, 1, (byte) Math.addExact(reserveClass, 1)};
        return store.scan(ColumnFamily.META, lower, upper, 2);
    }

    private CapacityVector mutateControlReserve(
            final int reserveClass, final CapacityVector amount, final boolean add) {
        validateMutableControlReserveClass(reserveClass);
        validateControlReservePartition(
                amount,
                reserveClass == CONTROL_RESERVE_SYSTEM_WRITER_CLASS,
                reserveClass == CONTROL_RESERVE_SYSTEM_WRITER_CLASS ? "system-writer" : "control reserve");
        if (capacityEnvelope == null) {
            throw new IllegalStateException("capacity envelope is required for control reserve accounting");
        }
        final CapacityGrant grant = controlReserveGrant(reserveClass);
        final CapacityVector current = controlReserveUsage.getOrDefault(reserveClass, CapacityVector.empty());
        final CapacityVector next = add ? current.add(amount) : current.subtract(amount);
        final CapacityVector sibling = reserveClass == CONTROL_RESERVE_NON_OUTCOME_CLASS
                ? controlReserveUsage.getOrDefault(CONTROL_RESERVE_SYSTEM_WRITER_CLASS, CapacityVector.empty())
                : reserveClass == CONTROL_RESERVE_SYSTEM_WRITER_CLASS
                        ? controlReserveUsage.getOrDefault(CONTROL_RESERVE_NON_OUTCOME_CLASS, CapacityVector.empty())
                        : CapacityVector.empty();
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

    private CapacityGrant controlReserveGrant(final int reserveClass) {
        return switch (reserveClass) {
            case CONTROL_RESERVE_NON_OUTCOME_CLASS, CONTROL_RESERVE_SYSTEM_WRITER_CLASS ->
                capacityEnvelope.nonOutcomeControl();
            case CONTROL_RESERVE_RECOVERY_CLASS -> capacityEnvelope.recoveryWorking();
            case CONTROL_RESERVE_EMERGENCY_CLASS -> capacityEnvelope.emergencyHeadroom();
            default -> throw new IllegalArgumentException("unsupported mutable control reserve class: " + reserveClass);
        };
    }

    private static void validateMutableControlReserveClass(final int reserveClass) {
        if (reserveClass < CONTROL_RESERVE_NON_OUTCOME_CLASS || reserveClass > CONTROL_RESERVE_SYSTEM_WRITER_CLASS) {
            throw new IllegalArgumentException("only CONTROL_RESERVE classes 3-6 are mutable locally");
        }
    }

    /**
     * Class 6 is a projection of the Route Broker system-writer budget. It
     * shares the immutable NON_OUTCOME_CONTROL grant with class 3, but the
     * two projections must remain dimension-disjoint so the same grant cannot
     * be charged twice. The actual Broker quota authority remains outside
     * this shard-local persistence primitive.
     */
    private static void validateControlReservePartition(
            final CapacityVector vector, final boolean systemWriter, final String name) {
        Objects.requireNonNull(vector, "vector");
        for (CapacityDimension dimension : CapacityDimension.values()) {
            final boolean writerDimension = dimension == CapacityDimension.SYSTEM_WRITER_RESERVED_RECORDS
                    || dimension == CapacityDimension.SYSTEM_WRITER_RESERVED_BYTES
                    || dimension == CapacityDimension.SYSTEM_WRITER_RESERVED_BYTES_PER_SECOND;
            if (writerDimension != systemWriter && vector.amount(dimension) != 0) {
                throw new IllegalArgumentException(
                        name + " projection contains an out-of-partition dimension: " + dimension);
            }
        }
    }

    private static void ensureNonOutcomeProjectionFits(
            final CapacityGrant grant, final CapacityVector nonOutcomeUsage, final CapacityVector systemWriterUsage) {
        if (!grant.vector().covers(nonOutcomeUsage.add(systemWriterUsage))) {
            throw new IllegalStateException("non-outcome and system-writer projections exceed immutable grant");
        }
    }

    private static OutcomeReserveUsage outcomeReserveUsage(final CapacityVector vector) {
        final long records = Math.addExact(
                Math.addExact(
                        vector.amount(CapacityDimension.RESULT_RECORDS),
                        vector.amount(CapacityDimension.SYSTEM_MUTATION_RECORDS)),
                vector.amount(CapacityDimension.EVIDENCE_RECORDS));
        final long bytes = Math.addExact(
                Math.addExact(
                        vector.amount(CapacityDimension.RESULT_BYTES),
                        vector.amount(CapacityDimension.SYSTEM_MUTATION_BYTES)),
                Math.addExact(
                        vector.amount(CapacityDimension.OUTCOME_WAL_BYTES),
                        vector.amount(CapacityDimension.EVIDENCE_BYTES)));
        return new OutcomeReserveUsage(records, bytes);
    }

    /**
     * Builds the Registry class-2 aggregate from the local projections that
     * have an exact durable ledger. The per-Lane map owns the logical and
     * inflight dimensions; the open-attempt projection owns outcome result,
     * system-mutation, WAL and evidence dimensions. External/physical
     * dimensions remain zero until their ledgers are wired into the shard.
     */
    private static CapacityVector aggregateQuotaUsage(
            final LaneQuotaUsageProjection lanes, final CapacityVector outcome) {
        Objects.requireNonNull(lanes, "lanes");
        Objects.requireNonNull(outcome, "outcome");
        final long[] amounts = new long[CapacityDimension.COUNT];
        for (LaneQuotaUsageEntry entry : lanes.map().entries()) {
            final long[] laneAmounts = entry.usage().toCapacityVector().amounts();
            for (int index = 0; index < 17; index++) {
                try {
                    amounts[index] = Math.addExact(amounts[index], laneAmounts[index]);
                } catch (ArithmeticException exception) {
                    throw new IllegalStateException("quota aggregate arithmetic overflow", exception);
                }
            }
        }
        final long[] outcomeAmounts = outcome.amounts();
        for (int index = CapacityDimension.RESULT_RECORDS.wireValue() - 1;
                index <= CapacityDimension.EVIDENCE_BYTES.wireValue() - 1;
                index++) {
            try {
                amounts[index] = Math.addExact(amounts[index], outcomeAmounts[index]);
            } catch (ArithmeticException exception) {
                throw new IllegalStateException("quota aggregate arithmetic overflow", exception);
            }
        }
        return new CapacityVector(amounts);
    }

    public synchronized MessageRecord getMessage(final DelayMessageId messageId) {
        Objects.requireNonNull(messageId, "messageId");
        requireMessageShard(messageId, "message lookup");
        final var value = messageIndexValue(messageId);
        if (value == null) {
            return null;
        }
        if (RetiredMessageIdentityRecord.isEncoded(value.payload())) {
            // The same id_cf/MESSAGE key may hold the compact identity branch;
            // callers that need to distinguish it use getRetiredMessageIdentity.
            RetiredMessageIdentityRecord.decode(value.payload());
            return null;
        }
        return validateMessageSourcePosition(messageId, MessageRecord.decode(value.payload()), "message lookup");
    }

    /** Returns the compact identity fence retained after Message history GC. */
    public synchronized RetiredMessageIdentityRecord getRetiredMessageIdentity(final DelayMessageId messageId) {
        Objects.requireNonNull(messageId, "messageId");
        requireMessageShard(messageId, "retired Message identity lookup");
        final var value = messageIndexValue(messageId);
        if (value == null || !RetiredMessageIdentityRecord.isEncoded(value.payload())) {
            return null;
        }
        final RetiredMessageIdentityRecord retired = RetiredMessageIdentityRecord.decode(value.payload());
        if (!retired.messageId().equals(messageId)) {
            throw new IllegalStateException("retired Message key/value identity mismatch");
        }
        validateSourcePositionShard(retired.appliedSourcePosition(), "retired Message identity lookup");
        return retired;
    }

    private ValueEnvelope.Decoded messageIndexValue(final DelayMessageId messageId) {
        final byte[] raw = store.get(ColumnFamily.ID, KeyCodec.idMessage(messageId));
        if (raw == null) {
            return null;
        }
        final ValueEnvelope.Decoded value = ValueEnvelope.decodeAny(raw);
        if (value.valueType() != MESSAGE_VALUE_TYPE) {
            throw new IllegalStateException("MESSAGE key has an unregistered value type: " + value.valueType());
        }
        return value;
    }

    /**
     * Converts a terminal Message index into the compact identity branch.
     * This is the shard-local half of Message history GC: all terminal
     * generations and their local DLQ outboxes are removed in the same batch,
     * while the identity fence remains until a later Floor/time-fence proof.
     * Provider/object-store and Recovery-Floor authority are deliberately not
     * inferred here.
     */
    synchronized RetiredMessageIdentityRecord retireMessageIdentity(
            final DelayMessageId messageId, final long messageIdentityReuseUntilEpochMs) {
        Objects.requireNonNull(messageId, "messageId");
        requireMessageShard(messageId, "Message identity retirement");
        if (messageIdentityReuseUntilEpochMs < messageId.routingId().logicalTimestampEpochMs()) {
            throw new IllegalArgumentException("Message identity reuse deadline precedes UUIDv7 timestamp");
        }
        final RetiredMessageIdentityRecord alreadyRetired = getRetiredMessageIdentity(messageId);
        if (alreadyRetired != null) {
            if (alreadyRetired.messageIdentityReuseUntilEpochMs() != messageIdentityReuseUntilEpochMs) {
                throw new IllegalStateException("retired Message identity deadline conflict");
            }
            return alreadyRetired;
        }
        final MessageRecord current = getMessage(messageId);
        if (current == null) {
            throw new IllegalStateException("cannot retire an unknown Message identity");
        }
        if (!isTerminalStatus(current.status())
                || current.runtimeIndex().currentWorkKind() != CurrentSendWorkKind.NONE
                || !current.runtimeIndex().attemptObligations().isEmpty()) {
            throw new IllegalStateException("Message identity still has active or open-obligation state");
        }
        if (lastAppliedSourcePosition == null || mutationSequence == 0) {
            throw new IllegalStateException("Message identity retirement lacks a durable source barrier");
        }
        final int limit = boundedLimitPlusOne(config.maxPendingMessages());
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> terminalEntries =
                store.scan(ColumnFamily.TERMINAL, new byte[] {1, 1}, new byte[] {2, 1}, limit);
        if (terminalEntries.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException(
                    "terminal history scan exceeded configured bound during Message retirement");
        }
        final List<TerminalGenerationRecord> histories = new ArrayList<>();
        for (var entry : terminalEntries) {
            if (entry.key().length != 2 + DelayMessageId.LENGTH + Integer.BYTES
                    || entry.key()[0] != 1
                    || entry.key()[1] != 1) {
                throw new IllegalStateException("invalid terminal key during Message identity retirement");
            }
            final byte[] messageBytes = Arrays.copyOfRange(entry.key(), 2, 2 + DelayMessageId.LENGTH);
            if (!Arrays.equals(messageBytes, messageId.bytes())) {
                continue;
            }
            final TerminalGenerationRecord history = TerminalGenerationRecord.decode(
                    ValueEnvelope.decode(entry.value(), 1).payload());
            if (!history.messageId().equals(messageId)
                    || !history.openObligations().isEmpty()) {
                throw new IllegalStateException("terminal history identity or obligation mismatch during retirement");
            }
            histories.add(history);
        }
        if (histories.isEmpty()
                || histories.stream()
                        .noneMatch(history ->
                                history.generation() == current.generation() && history.status() == current.status())) {
            throw new IllegalStateException("current terminal Message history is missing during retirement");
        }
        final RetiredMessageIdentityRecord retired = new RetiredMessageIdentityRecord(
                messageId,
                messageIdentityReuseUntilEpochMs,
                mutationSequence,
                lastAppliedSourcePosition.canonicalBytes());
        store.write(batch -> {
            for (TerminalGenerationRecord history : histories) {
                batch.delete(ColumnFamily.TERMINAL, KeyCodec.terminalGeneration(messageId, history.generation()));
                if (history.status() == MessageStatus.DEAD_LETTER) {
                    final byte[] exportId =
                            DlqExportRecord.deriveId(messageId, history.generation(), history.stateVersion());
                    batch.delete(ColumnFamily.TERMINAL, KeyCodec.terminalDlqExport(exportId));
                }
            }
            batch.delete(ColumnFamily.ID, KeyCodec.idScheduleBinding(messageId));
            batch.putValue(ColumnFamily.ID, MESSAGE_VALUE_TYPE, KeyCodec.idMessage(messageId), retired.encode());
        });
        return retired;
    }

    /** Result of the local necessary-condition check for deleting a retired identity. */
    public enum MessageIdentityGcDecision {
        NO_TOMBSTONE,
        SOURCE_FENCE_NOT_CLOSED,
        FLOOR_NOT_COVERING,
        COMPACTED
    }

    /**
     * Deletes a retired identity only after the source fence and the supplied
     * local Recovery Catalog prove that the retirement barrier is covered.
     * Catalog/session CAS, identity-retention policy and provider quiescence
     * remain external gates; any authority failure returns a conservative
     * {@link MessageIdentityGcDecision#FLOOR_NOT_COVERING} result.
     */
    synchronized MessageIdentityGcDecision compactRetiredMessageIdentity(
            final DelayMessageId messageId,
            final RecoveryCatalogAuthority catalog,
            final byte[] candidateCheckpointId) {
        Objects.requireNonNull(messageId, "messageId");
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(candidateCheckpointId, "candidateCheckpointId");
        requireMessageShard(messageId, "retired Message identity compaction");
        final RetiredMessageIdentityRecord retired = getRetiredMessageIdentity(messageId);
        if (retired == null) {
            return MessageIdentityGcDecision.NO_TOMBSTONE;
        }
        if (closedIngressDeadlineThrough < retired.messageIdentityReuseUntilEpochMs()) {
            return MessageIdentityGcDecision.SOURCE_FENCE_NOT_CLOSED;
        }
        final SourcePosition retirementPosition;
        try {
            retirementPosition = SourcePositionCodec.decode(retired.appliedSourcePosition());
            if (catalog.proveFloorCoverage(
                            candidateCheckpointId, retired.retirementMutationSequence(), retirementPosition)
                    .isEmpty()) {
                return MessageIdentityGcDecision.FLOOR_NOT_COVERING;
            }
        } catch (RuntimeException unavailableOrInvalidAuthority) {
            return MessageIdentityGcDecision.FLOOR_NOT_COVERING;
        }
        store.write(batch -> batch.delete(ColumnFamily.ID, KeyCodec.idMessage(messageId)));
        return MessageIdentityGcDecision.COMPACTED;
    }

    /** Returns the exact accepted Registry Schedule/Prepare binding, if any. */
    public synchronized ScheduleBinding getScheduleBinding(final DelayMessageId messageId) {
        Objects.requireNonNull(messageId, "messageId");
        if (!store.shardId().equals(messageId.routingId().shardId())) {
            throw new IllegalStateException(" Schedule binding key shard mismatch");
        }
        final var value = store.getValue(ColumnFamily.ID, KeyCodec.idScheduleBinding(messageId), 4);
        if (value == null) {
            return null;
        }
        final ScheduleBinding binding = ScheduleBinding.decode(value.payload());
        if (!binding.delayMessageId().equals(messageId)) {
            throw new IllegalStateException(" Schedule binding key/value identity mismatch");
        }
        final MessageRecord message = getMessage(messageId);
        if (message != null && !message.laneId().equals(binding.laneId())) {
            throw new IllegalStateException(" Schedule binding Lane does not match message");
        }
        return binding;
    }

    /**
     * Derives the replay-stable Claim materialization from the accepted
     * Schedule binding and the current durable Message generation.
     *
     * <p>This closes the local durable projection boundary only. It does not
     * infer live Profile readiness, serialization, credentials, channel
     * leases, or a Claim charge; those remain explicit Worker prerequisite
     * gates. A missing binding, current timeline, or committed object proof
     * fails closed rather than falling back to a compatibility projection.</p>
     */
    public synchronized ClaimMaterialization resolveClaimMaterialization(final DelayMessageId messageId) {
        final DelayMessageId exactMessageId = Objects.requireNonNull(messageId, "messageId");
        final MessageRecord current = getMessage(exactMessageId);
        if (current == null) {
            throw new IllegalStateException("Claim materialization requires an existing Message");
        }
        if (current.status() != MessageStatus.SCHEDULED) {
            throw new IllegalStateException("Claim materialization requires a scheduled Message");
        }
        final ScheduleBinding binding = getScheduleBinding(exactMessageId);
        if (binding == null) {
            throw new IllegalStateException("automatic Claim materialization requires a Schedule binding");
        }
        final CanonicalScheduleIntent intent;
        final PayloadForPublish payload;
        if (binding.commandType() == CommandType.SCHEDULE) {
            intent = ScheduleCommandBody.decode(binding.canonicalBody()).intent();
            payload = intent.hasInlinePayload()
                    ? PayloadForPublish.inline(current.payload())
                    : PayloadForPublish.object(intent.committedPayload());
        } else if (binding.commandType() == CommandType.PREPARE_LARGE_SCHEDULE) {
            final PrepareLargeScheduleBody prepare = PrepareLargeScheduleBody.decode(binding.canonicalBody());
            intent = prepare.intentWithoutPayload();
            final PayloadReference reference = current.payloadReference();
            if (reference == null
                    || !Bytes.constantTimeEquals(
                            reference.objectStoreProfileHash(),
                            prepare.objectStoreProfile().semanticHash())
                    || !reference.hasCommitIdentity()) {
                throw new IllegalStateException("committed PrepareLarge payload proof is unavailable");
            }
            final CommittedPayloadDescriptor descriptor = new CommittedPayloadDescriptor(
                    prepare.objectStoreProfile(),
                    reference.container(),
                    reference.objectKey(),
                    reference.immutableObjectVersion(),
                    reference.etag(),
                    reference.length(),
                    reference.payloadSha256(),
                    reference.reservationId(),
                    reference.proofId());
            payload = PayloadForPublish.object(descriptor);
        } else {
            throw new IllegalStateException("unsupported Claim binding command type");
        }
        final TimelineWorkRef timeline = current.runtimeIndex().timeline();
        if (timeline == null) {
            throw new IllegalStateException("scheduled Message has no durable TimelineWorkRef");
        }
        final CanonicalLaneTuple.Projection lane = CanonicalLaneTuple.project(binding.canonicalLaneTuple());
        final ClaimMaterialization materialization = intent.legacyPolicyDefault()
                ? new ClaimMaterialization(
                        lane.destinationProfile(),
                        lane.capabilityProfile(),
                        lane.targetResource(),
                        lane.physicalPartition(),
                        exactMessageId,
                        Integer.toUnsignedLong(current.generation()),
                        payload,
                        intent.adapterMetadata(),
                        current.deliverAtEpochMs(),
                        current.expireAtEpochMs(),
                        timeline.actionAtEpochMs())
                : new ClaimMaterialization(
                        lane.destinationProfile(),
                        lane.capabilityProfile(),
                        lane.targetResource(),
                        lane.physicalPartition(),
                        exactMessageId,
                        Integer.toUnsignedLong(current.generation()),
                        payload,
                        intent.adapterMetadata(),
                        current.deliverAtEpochMs(),
                        current.expireAtEpochMs(),
                        timeline.actionAtEpochMs(),
                        intent.nativeDeliveryPolicy(),
                        intent.eventTimeEpochMs(),
                        null);
        requireClaimMaterializationMatchesMessage(exactMessageId, current, materialization);
        return materialization;
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
        return new MessageQuerySnapshot(
                messageId,
                current.generation(),
                current.stateVersion(),
                state,
                current.deliverAtEpochMs(),
                current.expireAtEpochMs(),
                payloadAvailability(current),
                current.runtimeIndex().possibleDestinationDuplicate(),
                terminalCode,
                terminalCode == null ? DlqExportState.NOT_CONFIGURED : dlqExportState(messageId, current.generation()));
    }

    /** Returns the durable local DLQ export outbox for one terminal generation. */
    public synchronized DlqExportRecord getDlqExportRecord(final DelayMessageId messageId, final int generation) {
        Objects.requireNonNull(messageId, "messageId");
        final TerminalGenerationRecord terminal = getTerminalGeneration(messageId, generation);
        if (terminal == null || terminal.status() != MessageStatus.DEAD_LETTER) {
            return null;
        }
        final byte[] exportId = DlqExportRecord.deriveId(messageId, generation, terminal.stateVersion());
        final ValueEnvelope.Decoded value =
                store.getValue(ColumnFamily.TERMINAL, KeyCodec.terminalDlqExport(exportId), DlqExportRecord.VALUE_TYPE);
        if (value == null) {
            return null;
        }
        final DlqExportRecord result = DlqExportRecord.decode(value.payload());
        if (!result.messageId().equals(messageId)
                || result.generation() != generation
                || result.terminalRevision() != terminal.stateVersion()) {
            throw new IllegalStateException("DLQ export record does not match terminal generation");
        }
        validateSourcePositionShard(result.appliedSourcePosition(), "DLQ export lookup");
        return result;
    }

    /** Returns the durable export state, with legacy terminals defaulting to NOT_CONFIGURED. */
    public synchronized DlqExportState dlqExportState(final DelayMessageId messageId, final int generation) {
        final DlqExportRecord record = getDlqExportRecord(messageId, generation);
        return record == null ? DlqExportState.NOT_CONFIGURED : record.state();
    }

    /** Returns the exact local Claim at an Owner Epoch, or {@code null} when it is no longer live. */
    public synchronized ClaimRecord getClaim(final byte[] claimId, final long ownerEpoch) {
        Bytes.requireLength(claimId, ClaimRecord.HASH_LENGTH, "claimId");
        if (ownerEpoch == 0) {
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
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> entries = store.scan(
                ColumnFamily.INFLIGHT,
                new byte[] {INFLIGHT_CLAIMED_KIND, 1},
                new byte[] {INFLIGHT_PUBLISHING_KIND, 1},
                limit);
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
     * Claims a scheduled message with the typed materialization projection.
     *
     * <p>The package-local byte-array primitive remains available to runtime
     * implementation and tests only. This entrypoint binds the replay-stable
     * projection to the current Message before the Claim batch is built, so a
     * caller cannot accidentally prepare a different message generation,
     * delivery window or payload under an otherwise valid canonical
     * materialization.</p>
     */
    public synchronized ClaimRecord claimForPublish(
            final DelayMessageId messageId,
            final AuthorIdentity owner,
            final long claimDeadlineEpochMs,
            final ClaimMaterialization materialization,
            final byte[] claimedCharge) {
        Objects.requireNonNull(materialization, "materialization");
        final MessageRecord current = getMessage(Objects.requireNonNull(messageId, "messageId"));
        requireClaimMaterializationMatchesMessage(messageId, current, materialization);
        return claimForPublish(messageId, owner, claimDeadlineEpochMs, materialization.canonicalBytes(), claimedCharge);
    }

    /**
     * Atomically takes a scheduled timeline item into a reversible local Claim.
     * This embedded method deliberately exposes no Producer call: admission must
     * later be represented by the source-ordered PUBLISH_ADMISSION mutation.
     */
    synchronized ClaimRecord claimForPublish(
            final DelayMessageId messageId,
            final AuthorIdentity owner,
            final long claimDeadlineEpochMs,
            final byte[] materialization,
            final byte[] claimedCharge) {
        Objects.requireNonNull(messageId, "messageId");
        Objects.requireNonNull(owner, "owner");
        // A shared Worker resource breach fences the next Claim/Admission
        // attempt across all owned shard DBs. Source-ordered control and
        // capacity-gated mutations remain applicable so drain/release work
        // can make progress; this pre-Producer helper must not create new
        // business work after the runtime envelope has failed.
        store.sharedResources().requireRuntimeBusinessAdmission();
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
        final long nextClaimSequence = nextUnsignedSequence(claimSequence, "Claim sequence");
        final byte[] claimId = Bytes.sha256(
                Bytes.utf8("nereus-delay-claim-id\0"),
                store.metadata().storeIncarnation(),
                Bytes.u64beBits(owner.generation()),
                Bytes.u64beBits(nextClaimSequence),
                messageId.bytes(),
                Bytes.u32beBits(current.generation()),
                Bytes.u64be(lane.laneVersion()));
        final TimelineWorkRef currentTimeline = current.runtimeIndex().timeline();
        final int workKind = currentTimeline != null && Arrays.equals(currentTimeline.encodedTimelineKey(), timelineKey)
                ? currentTimeline.workKind().wireValue()
                : current.retryEligibilityAtEpochMs() == current.deliverAtEpochMs() ? 1 : 2;
        final byte[] precondition = buildClaimPrecondition(
                claimId,
                messageId,
                current,
                lane,
                timelineKey,
                owner,
                claimDeadlineEpochMs,
                materialization,
                claimedCharge,
                workKind);
        MessageRecord next = MessageRecord.current(
                MessageStatus.CLAIMED,
                current.generation(),
                Math.addExact(current.stateVersion(), 1),
                current.deliverAtEpochMs(),
                current.expireAtEpochMs(),
                current.laneId(),
                current.orderingMode(),
                current.payload(),
                current.scheduleSourcePosition(),
                current.payloadReference(),
                current.retryEligibilityAtEpochMs());
        final ClaimRecord claim = ClaimRecord.claimed(
                messageId,
                current.generation(),
                claimId,
                owner.generation(),
                nextClaimSequence,
                current.laneId(),
                lane.laneIncarnation(),
                lane.laneControlVersion(),
                lane.laneVersion(),
                owner.asOwnerIdentity().canonicalBytes(),
                store.metadata().storeIncarnation(),
                precondition,
                timelineKey,
                next.stateVersion(),
                currentTimeline == null ? null : currentTimeline.canonicalBytes());
        next = next.withRuntimeIndex(GenerationRuntimeIndex.claimed(
                claim.claimId(),
                current.runtimeIndex().attemptObligations(),
                current.runtimeIndex().admissionsUsed(),
                current.runtimeIndex().uncertainRetryAdmissionsUsed(),
                current.runtimeIndex().possibleDestinationDuplicate(),
                next.stateVersion()));
        final MessageRecord claimedNext = next;
        final LaneQuotaUsageProjection nextLaneQuota = addClaimQuotaUsage(claim);
        final SourcePosition schedulePosition = SourcePositionCodec.decode(current.scheduleSourcePosition());
        final Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections =
                readyProjections(schedulePosition, messageId, current, next, null, nextLaneQuota);
        store.write(batch -> {
            batch.delete(ColumnFamily.TIMELINE, timelineKey);
            batch.putValue(ColumnFamily.INFLIGHT, ClaimRecord.VALUE_TYPE, claim.encodedKey(), claim.encode());
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(messageId), claimedNext.encode());
            batch.putValue(
                    ColumnFamily.META, 1, KeyCodec.metaFixed(META_CLAIM_SEQUENCE), Bytes.u64beBits(nextClaimSequence));
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            persistQuota(batch, quota, nextLaneQuota);
        });
        claimSequence = nextClaimSequence;
        laneQuotaUsage = nextLaneQuota;
        return claim;
    }

    private void requireClaimMaterializationMatchesMessage(
            final DelayMessageId messageId, final MessageRecord current, final ClaimMaterialization materialization) {
        if (current == null) {
            throw new IllegalStateException("Claim materialization requires an existing Message");
        }
        if (!materialization.messageId().equals(messageId)) {
            throw new IllegalArgumentException("Claim materialization message identity mismatch");
        }
        if (materialization.generation() != Integer.toUnsignedLong(current.generation())) {
            throw new IllegalArgumentException("Claim materialization generation mismatch");
        }
        if (materialization.deliverAtEpochMs() != current.deliverAtEpochMs()
                || materialization.expireAtEpochMs() != current.expireAtEpochMs()) {
            throw new IllegalArgumentException("Claim materialization delivery window mismatch");
        }
        final TimelineWorkRef currentWork = current.runtimeIndex().timeline();
        if (currentWork == null || materialization.actionAtEpochMs() != currentWork.actionAtEpochMs()) {
            throw new IllegalArgumentException("Claim materialization action time mismatch");
        }

        final PayloadForPublish payload = materialization.payload();
        if (payload.length() != current.payloadLength()) {
            throw new IllegalArgumentException("Claim materialization payload length mismatch");
        }
        if (payload.hasInlinePayload()) {
            if (current.payloadReference() != null || !Arrays.equals(payload.inlinePayload(), current.payload())) {
                throw new IllegalArgumentException("Claim materialization inline payload mismatch");
            }
        } else if (current.payloadReference() == null
                || !PayloadReference.fromDescriptor(payload.object()).equals(current.payloadReference())) {
            throw new IllegalArgumentException("Claim materialization object payload mismatch");
        }
        requireClaimMaterializationMatchesBinding(messageId, materialization);
    }

    private void requireClaimMaterializationMatchesBinding(
            final DelayMessageId messageId, final ClaimMaterialization materialization) {
        final ScheduleBinding binding = getScheduleBinding(messageId);
        if (binding == null) {
            return;
        }
        final CanonicalScheduleIntent intent;
        final PrepareLargeScheduleBody prepare;
        if (binding.commandType() == CommandType.SCHEDULE) {
            intent = ScheduleCommandBody.decode(binding.canonicalBody()).intent();
            prepare = null;
        } else if (binding.commandType() == CommandType.PREPARE_LARGE_SCHEDULE) {
            prepare = PrepareLargeScheduleBody.decode(binding.canonicalBody());
            intent = prepare.intentWithoutPayload();
        } else {
            throw new IllegalStateException("unsupported Claim binding command type");
        }
        if (!materialization.destinationProfile().equals(intent.profile())) {
            throw new IllegalArgumentException("Claim materialization Destination Profile mismatch");
        }
        if (!materialization.businessMetadata().equals(intent.adapterMetadata())) {
            throw new IllegalArgumentException("Claim materialization business metadata mismatch");
        }
        if (materialization.deliverAtEpochMs() != intent.deliverAtEpochMs()
                || materialization.expireAtEpochMs() != intent.expireAtEpochMs()) {
            throw new IllegalArgumentException("Claim materialization delivery window mismatch");
        }
        binding.requireClaimLaneProjection(materialization);
        final PayloadForPublish payload = materialization.payload();
        if (prepare == null) {
            if (intent.hasInlinePayload()) {
                if (!payload.hasInlinePayload() || !Arrays.equals(payload.inlinePayload(), intent.inlinePayload())) {
                    throw new IllegalArgumentException("Claim materialization inline payload mismatch");
                }
            } else if (payload.hasInlinePayload() || !payload.object().equals(intent.committedPayload())) {
                throw new IllegalArgumentException("Claim materialization object payload mismatch");
            }
        } else if (payload.hasInlinePayload()
                || !payload.object().objectStoreProfile().equals(prepare.objectStoreProfile())
                || payload.object().length() != prepare.expectedPayloadLength()
                || !Arrays.equals(payload.object().payloadSha256(), prepare.payloadSha256())) {
            throw new IllegalArgumentException("Claim materialization Prepare payload binding mismatch");
        }
    }

    /** Atomically revokes a local Claim and restores its exact timeline work. */
    synchronized MessageRecord revokeClaim(final byte[] claimId, final long ownerEpoch) {
        final ClaimRecord claim = getClaim(claimId, ownerEpoch);
        if (claim == null) {
            return null;
        }
        final MessageRecord current = getMessage(claim.delayMessageId());
        if (current == null
                || current.status() != MessageStatus.CLAIMED
                || current.generation() != claim.generation()
                || current.stateVersion() != claim.runtimeRevision()) {
            throw new IllegalStateException("Claim does not match current CLAIMED message");
        }
        final ClaimResultBody.ClaimPrecondition precondition =
                ClaimResultBody.decodePrecondition(claim.preconditionBytes());
        final TimelineWorkKind workKind = TimelineWorkKind.fromWire(precondition.sourceWorkKind());
        final MessageRecord revokedNext = restoreClaimedMessageToTimeline(claim, current, workKind);
        final LaneQuotaUsageProjection nextLaneQuota = removeClaimQuotaUsage(claim);
        final SourcePosition schedulePosition = SourcePositionCodec.decode(current.scheduleSourcePosition());
        final Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections =
                readyProjections(schedulePosition, claim.delayMessageId(), current, revokedNext, null, nextLaneQuota);
        store.write(batch -> {
            batch.delete(ColumnFamily.INFLIGHT, claim.encodedKey());
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(claim.delayMessageId()), revokedNext.encode());
            batch.putValue(
                    ColumnFamily.TIMELINE,
                    1,
                    claim.timelineKey(),
                    encodeTimelineValue(claim.delayMessageId(), revokedNext));
            batch.putValue(
                    ColumnFamily.TIMELINE,
                    1,
                    expiryKey(claim.delayMessageId(), revokedNext),
                    encodeTimelineValue(claim.delayMessageId(), revokedNext));
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            persistQuota(batch, quota, nextLaneQuota);
        });
        laneQuotaUsage = nextLaneQuota;
        return revokedNext;
    }

    /**
     * Restores a CLAIMED message from the exact timeline snapshot retained by
     * its Claim. The Claim's source work is the only replay-stable authority
     * while the current runtime branch carries only the Claim identity; using
     * a fresh compatibility MessageRecord would otherwise lose an early
     * actionAt when the resolver/catalog is unavailable during rollback.
     */
    private MessageRecord restoreClaimedMessageToTimeline(
            final ClaimRecord claim, final MessageRecord current, final TimelineWorkKind workKind) {
        MessageRecord next = MessageRecord.current(
                MessageStatus.SCHEDULED,
                current.generation(),
                Math.addExact(current.stateVersion(), 1),
                current.deliverAtEpochMs(),
                current.expireAtEpochMs(),
                current.laneId(),
                current.orderingMode(),
                current.payload(),
                current.scheduleSourcePosition(),
                current.payloadReference(),
                current.retryEligibilityAtEpochMs());
        final byte[] sourceTimelineWork = claim.sourceTimelineWork();
        if (workKind == TimelineWorkKind.UNCERTAIN_RETRY && sourceTimelineWork.length == 0) {
            throw new IllegalStateException("legacy Claim lacks UNCERTAIN_RETRY source work projection");
        }
        if (sourceTimelineWork.length == 0) {
            next = next.withRuntimeIndex(timelineRuntimeIndex(
                    claim.delayMessageId(),
                    next,
                    workKind,
                    Math.addExact(current.runtimeIndex().admissionsUsed(), 1),
                    next.stateVersion(),
                    UncertainRetryAuthority.NONE,
                    null,
                    null,
                    current.runtimeIndex()));
        } else {
            final TimelineWorkRef priorWork = TimelineWorkRef.decode(sourceTimelineWork);
            if (priorWork.workKind() != workKind
                    || !Arrays.equals(priorWork.encodedTimelineKey(), claim.timelineKey())) {
                throw new IllegalStateException("Claim source timeline does not match its precondition");
            }
            if (priorWork.retryEligibilityAtEpochMs() != current.retryEligibilityAtEpochMs()) {
                throw new IllegalStateException("Claim source timeline retry gate does not match current Message");
            }
            final TimelineWorkRef restoredWork = new TimelineWorkRef(
                    priorWork.workKind(),
                    priorWork.encodedTimelineKey(),
                    priorWork.actionAtEpochMs(),
                    priorWork.retryEligibilityAtEpochMs(),
                    priorWork.candidateAttemptNo(),
                    next.stateVersion(),
                    priorWork.orderedHeadBlocking(),
                    priorWork.uncertainRetryAuthority(),
                    priorWork.uncertainRetryControl(),
                    priorWork.uncertainRetryControlPosition());
            final GenerationAggregateState requestedAggregate =
                    switch (workKind) {
                        case INITIAL_SCHEDULE -> GenerationAggregateState.SCHEDULED;
                        case DEFINITIVE_RETRY -> GenerationAggregateState.RETRY_WAIT;
                        case UNCERTAIN_RETRY -> GenerationAggregateState.UNCERTAIN;
                    };
            next = next.withRuntimeIndex(GenerationRuntimeIndex.timeline(
                    requestedAggregate,
                    restoredWork,
                    current.runtimeIndex().attemptObligations(),
                    current.runtimeIndex().admissionsUsed(),
                    current.runtimeIndex().uncertainRetryAdmissionsUsed(),
                    current.runtimeIndex().possibleDestinationDuplicate(),
                    next.stateVersion()));
        }
        if (!Arrays.equals(claim.timelineKey(), timelineKey(claim.delayMessageId(), next))) {
            throw new IllegalStateException("Claim timeline key is not reversible");
        }
        return next;
    }

    /**
     * Revokes every live reversible Claim owned by one Owner Epoch.
     *
     * <p>The bounded scan is performed while the shard single-writer lock is
     * held, so the discovered Claim identities cannot change between the scan
     * and the individual atomic rollback batches. A scan over the configured
     * bound fails closed instead of silently leaving an unknown Claim during
     * owner drain.</p>
     */
    public synchronized int revokeClaimsForOwner(final long ownerEpoch) {
        if (ownerEpoch == 0) {
            throw new IllegalArgumentException("ownerEpoch must be positive");
        }
        final int limit = boundedLimitPlusOne(config.maxPendingMessages());
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> entries = store.scan(
                ColumnFamily.INFLIGHT,
                new byte[] {INFLIGHT_CLAIMED_KIND, 1},
                new byte[] {INFLIGHT_PUBLISHING_KIND, 1},
                limit);
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

    /**
     * Requeues every live reversible Claim before a recovered owner is
     * activated. A Claim is a pre-Producer reservation, so a checkpoint may
     * legitimately reopen with {@code CLAIMED} work even though no Admission
     * exists in the replayed source prefix. Recovery must restore that work
     * to its exact semantic timeline key/work kind while assigning a checked
     * successor runtime revision; it must not treat the old Owner Epoch as a
     * current publishing authority.
     *
     * <p>The scan is bounded by the configured pending-message envelope and
     * fails closed on overflow. Each rollback uses the same synchronous
     * message/timeline/inflight/READY/quota WriteBatch as an owner drain, so a
     * crash cannot expose a half-requeued Claim. PUBLISHING/UNCERTAIN
     * attempt obligations are deliberately outside this method and remain
     * subject to source-ordered outcome recovery.</p>
     *
     * @return the number of Claims restored to timeline work
     */
    public synchronized int requeueClaimsForRecovery() {
        final int limit = boundedLimitPlusOne(config.maxPendingMessages());
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> entries = store.scan(
                ColumnFamily.INFLIGHT,
                new byte[] {INFLIGHT_CLAIMED_KIND, 1},
                new byte[] {INFLIGHT_PUBLISHING_KIND, 1},
                limit);
        if (entries.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("Claim scan exceeded configured bound during recovery");
        }
        final List<ClaimRecord> claims = new ArrayList<>(entries.size());
        for (var entry : entries) {
            claims.add(decodeClaim(entry));
        }
        int requeued = 0;
        for (ClaimRecord claim : claims) {
            if (revokeClaim(claim.claimId(), claim.ownerEpoch()) != null) {
                requeued++;
            }
        }
        return requeued;
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
        return validateReservationIdentity(
                reservationId, PayloadReservation.decode(value.payload()), "reservation lookup");
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
        final PayloadAvailability availability =
                switch (reservation.status()) {
                    case RESERVED -> PayloadAvailability.UPLOAD_PENDING;
                    case COMMITTED -> PayloadAvailability.OBJECT_RETAINED;
                    case ABANDONED, EXPIRED -> PayloadAvailability.NOT_APPLICABLE;
                };
        return new ReservationQuerySnapshot(
                reservation.reservationId(),
                reservation.delayMessageId(),
                reservation.stateVersion(),
                reservation.status(),
                reservation.reservationExpiryEpochMs(),
                availability);
    }

    /**
     * Materializes a reservation that has already been logically expired by a
     * persisted TIME_FENCE watermark. This is a local bounded cursor action,
     * not a new source-log decision.
     */
    synchronized PayloadReservation materializeReservationExpiry(final byte[] reservationId) {
        Bytes.requireLength(reservationId, 32, "reservationId");
        return materializeReservationExpiryInternal(reservationId, null).reservation();
    }

    /**
     * Materializes one exact scanner candidate after a bounded GC queue wait.
     * A candidate may become stale while it is queued because a source-ordered
     * Commit, Cancel or Lane Close changed the reservation. Such a candidate
     * is reported as a no-op result rather than being applied to a different
     * reservation projection.
     */
    public synchronized ReservationExpiryMaterializationResult materializeReservationExpiry(
            final ReservationExpiryWork candidate) {
        final ReservationExpiryWork expected = Objects.requireNonNull(candidate, "reservation expiry candidate");
        return materializeReservationExpiryInternal(expected.reservationId(), expected);
    }

    private ReservationExpiryMaterializationResult materializeReservationExpiryInternal(
            final byte[] reservationId, final ReservationExpiryWork expected) {
        Bytes.requireLength(reservationId, 32, "reservationId");
        final var value = store.getValue(ColumnFamily.ID, KeyCodec.idReservation(reservationId), 2);
        if (value == null) {
            return ReservationExpiryMaterializationResult.notFound();
        }
        final PayloadReservation current = validateReservationIdentity(
                reservationId, PayloadReservation.decode(value.payload()), "reservation expiry materialization");
        if (expected != null
                && (!current.shardId().equals(store.shardId())
                        || !current.delayMessageId().equals(expected.messageId())
                        || current.reservationExpiryEpochMs() != expected.reservationExpiryEpochMs()
                        || current.stateVersion() != expected.stateVersion())) {
            return ReservationExpiryMaterializationResult.stale(current);
        }
        final LaneRecord lane = readLane(current.intent().laneId());
        if (lane != null
                && lane.admissionGate() == AdmissionGate.CLOSED
                && current.status() == PayloadReservationStatus.RESERVED) {
            return ReservationExpiryMaterializationResult.alreadyTerminal(effectiveReservation(current));
        }
        if (current.status() != PayloadReservationStatus.RESERVED) {
            // A source-ordered transition may have won while this candidate
            // waited in GC. Remove only the exact stale index key; the
            // lifecycle state and quota have already been decided elsewhere.
            store.write(batch -> batch.delete(
                    ColumnFamily.TIMELINE,
                    KeyCodec.reservationExpiry(current.reservationExpiryEpochMs(), current.reservationId())));
            return ReservationExpiryMaterializationResult.alreadyTerminal(current);
        }
        if (closedIngressDeadlineThrough < current.reservationExpiryEpochMs()) {
            return ReservationExpiryMaterializationResult.stale(current);
        }
        final PayloadReservation expired = current.withLifecycle(
                PayloadReservationStatus.EXPIRED,
                Math.addExact(current.stateVersion(), 1),
                current.sourcePosition(),
                null);
        final ShardQuota nextQuota = quota.removeReservation(current.intent().expectedPayloadLength());
        final LaneQuotaUsageProjection nextLaneQuota = removeReservationQuotaUsage(current, nextQuota);
        store.write(batch -> {
            batch.putValue(ColumnFamily.ID, 2, KeyCodec.idReservation(expired.reservationId()), expired.encode());
            batch.delete(
                    ColumnFamily.TIMELINE,
                    KeyCodec.reservationExpiry(expired.reservationExpiryEpochMs(), expired.reservationId()));
            persistQuota(batch, nextQuota, nextLaneQuota);
        });
        quota = nextQuota;
        laneQuotaUsage = nextLaneQuota;
        return ReservationExpiryMaterializationResult.materialized(expired);
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
     * result. A query locator contains more than the command id; its command
     * hash must match the dedupe record before the result can be projected.
     */
    public synchronized boolean matchesCommandHash(final CommandId commandId, final byte[] expectedCommandHash) {
        Objects.requireNonNull(commandId, "commandId");
        Bytes.requireLength(expectedCommandHash, 32, "expectedCommandHash");
        requireCommandShard(commandId, "command identity lookup");
        final CommandDedupeRecord record = readCommandDedupe(commandId);
        return record != null && Bytes.constantTimeEquals(record.commandHash(), expectedCommandHash);
    }

    /**
     * Checks that an exact physical source position's POSITION audit names this
     * client command. A logical command result and matching hash are not enough
     * to authorize a queued receipt whose source position is the record locator.
     */
    public synchronized boolean matchesCommandPosition(final CommandId commandId, final SourcePosition sourcePosition) {
        Objects.requireNonNull(commandId, "commandId");
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        requireCommandShard(commandId, "command position lookup");
        if (!store.shardId().equals(sourcePosition.shardId())) {
            throw new IllegalArgumentException("command position does not belong to shard");
        }
        final PositionAudit audit = readPositionAudit(sourcePosition);
        return audit != null && audit.commandId() != null && audit.commandId().equals(commandId);
    }

    public synchronized SystemMutationResult getSystemMutationResult(final byte[] mutationId) {
        Bytes.requireLength(mutationId, SystemMutation.HASH_LENGTH, "mutationId");
        final var value = store.getValue(
                ColumnFamily.DEDUPE, KeyCodec.dedupeSystemMutation(mutationId), SystemMutationResult.VALUE_TYPE);
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
    public synchronized ResourceRetireIntentRecord getResourceRetireIntent(
            final ResourceKind resourceKind, final byte[] resourceIdentityHash, final long expectedVersion) {
        Objects.requireNonNull(resourceKind, "resourceKind");
        Bytes.requireLength(resourceIdentityHash, SystemMutation.HASH_LENGTH, "resourceIdentityHash");
        final byte[] raw = gcValue(resourceKind, resourceIdentityHash, expectedVersion);
        if (raw == null) {
            return null;
        }
        final int valueType = gcValueType(raw);
        if (valueType == ResourceRetireIntentRecord.VALUE_TYPE) {
            return validateGcIntentIdentity(
                    ResourceRetireIntentRecord.decode(ValueEnvelope.decode(raw, ResourceRetireIntentRecord.VALUE_TYPE)
                            .payload()),
                    resourceKind,
                    resourceIdentityHash,
                    expectedVersion);
        }
        if (valueType == ResourceDeleteConfirmedRecord.VALUE_TYPE) {
            return validateGcIntentIdentity(
                    ResourceDeleteConfirmedRecord.decode(
                                    ValueEnvelope.decode(raw, ResourceDeleteConfirmedRecord.VALUE_TYPE)
                                            .payload())
                            .retireIntent(),
                    resourceKind,
                    resourceIdentityHash,
                    expectedVersion);
        }
        throw new IllegalStateException("unknown gc task value type: " + valueType);
    }

    /** Returns the durable delete confirmation, if this exact task has reached that local state. */
    public synchronized ResourceDeleteConfirmedRecord getResourceDeleteConfirmation(
            final ResourceKind resourceKind, final byte[] resourceIdentityHash, final long expectedVersion) {
        Objects.requireNonNull(resourceKind, "resourceKind");
        Bytes.requireLength(resourceIdentityHash, SystemMutation.HASH_LENGTH, "resourceIdentityHash");
        final byte[] raw = gcValue(resourceKind, resourceIdentityHash, expectedVersion);
        if (raw == null || gcValueType(raw) != ResourceDeleteConfirmedRecord.VALUE_TYPE) {
            return null;
        }
        return validateGcConfirmationIdentity(
                ResourceDeleteConfirmedRecord.decode(ValueEnvelope.decode(raw, ResourceDeleteConfirmedRecord.VALUE_TYPE)
                        .payload()),
                resourceKind,
                resourceIdentityHash,
                expectedVersion);
    }

    /**
     * Physically removes a completed local GC task only after the exact
     * catalog-backed Floor proof is present. This is background compaction,
     * not a new Shard Log mutation; the source-ordered intent and confirmation
     * records have already supplied the durable audit boundary.
     */
    synchronized ResourceGcGuard.Decision compactResourceDeleteConfirmation(
            final ResourceKind resourceKind,
            final byte[] resourceIdentityHash,
            final long expectedVersion,
            final RecoveryCatalogAuthority catalog,
            final byte[] candidateCheckpointId) {
        Objects.requireNonNull(resourceKind, "resourceKind");
        Bytes.requireLength(resourceIdentityHash, SystemMutation.HASH_LENGTH, "resourceIdentityHash");
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(candidateCheckpointId, "candidateCheckpointId");
        final ResourceRetireIntentRecord intent =
                getResourceRetireIntent(resourceKind, resourceIdentityHash, expectedVersion);
        final ResourceDeleteConfirmedRecord confirmation =
                getResourceDeleteConfirmation(resourceKind, resourceIdentityHash, expectedVersion);
        final ResourceGcGuard.Decision decision =
                ResourceGcGuard.evaluate(intent, confirmation, catalog, candidateCheckpointId);
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
    public synchronized SystemMutationResult applySystemMutation(
            final SystemMutation mutation, final SourcePosition sourcePosition, final PublicKey verificationKey) {
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
                    if (!Arrays.equals(lastAppliedSourcePosition.canonicalBytes(), sourcePosition.canonicalBytes())) {
                        throw new IllegalStateException("duplicate source position has conflicting System Mutation");
                    }
                }
                if (order == 0) {
                    final PositionAudit audit = readPositionAudit(sourcePosition);
                    if (audit == null
                            || audit.systemMutationId() == null
                            || !Bytes.constantTimeEquals(audit.systemMutationId(), prior.mutationId())) {
                        throw new IllegalStateException(
                                "duplicate System Mutation source position has conflicting evidence");
                    }
                    if (systemMutationRetryWindowExpired(mutation, sourcePosition)) {
                        return expiredSystemMutationResult(mutation, sourcePosition);
                    }
                    // The exact mutation identity/hash plus the physical
                    // POSITION audit proves that this is a replay of the
                    // already-applied record (including a later duplicate
                    // whose WriteBatch advanced the shard cursor). Keep the
                    // first Source Position in the logical result, but do
                    // not execute the mutation again.
                    return prior;
                }
            }
            if (systemMutationRetryWindowExpired(mutation, sourcePosition)) {
                // Keep the first logical result immutable. A duplicate outside
                // its signed/fenced retry window has only a position-level
                // outcome and must not overwrite dedupe/SYSTEM_MUTATION.
                return persistExpiredSystemPositionOnly(mutation, sourcePosition);
            }
            if (!Arrays.equals(prior.appliedSourcePosition(), sourcePosition.canonicalBytes())) {
                store.write(batch -> {
                    writePosition(batch, sourcePosition);
                    batch.putValue(
                            ColumnFamily.DEDUPE,
                            DEDUPE_POSITION_VALUE_TYPE,
                            KeyCodec.dedupePosition(sourcePosition.canonicalBytes()),
                            prior.mutationId());
                });
                lastAppliedSourcePosition = sourcePosition;
                mutationSequence = nextMutationSequence();
            }
            return prior;
        }
        validateMutationPosition(sourcePosition);
        if (!mutation.verifySignature(verificationKey)) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        if (systemMutationRetryWindowExpired(mutation, sourcePosition)) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED);
        }
        if (controlTargetRegistrationAuthority != null && requiresControlTargetRegistration(mutation.type())) {
            final SystemMutationResult registrationResult = validateRegisteredControlMutation(mutation, sourcePosition);
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
                default ->
                    persistSystemResult(
                            mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
            };
        } catch (CommandResolutionException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED, exception.stableCode());
        } catch (ArithmeticException | IllegalArgumentException exception) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.STALE_SYSTEM_MUTATION);
        }
    }

    private SystemMutationResult validateRegisteredControlMutation(
            final SystemMutation mutation, final SourcePosition sourcePosition) {
        try {
            final ControlRef controlRef = controlRefFor(mutation);
            final PreparedControlOperation prepared = controlTargetRegistrationAuthority
                    .find(controlRef.operationId())
                    .orElse(null);
            if (prepared == null) {
                return persistSystemResult(
                        mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
            }
            final ControlTargetRef target = prepared.targets().stream()
                    .filter(candidate -> candidate.targetIndex() == controlRef.targetIndex())
                    .findFirst()
                    .orElse(null);
            if (target == null) {
                return persistSystemResult(
                        mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
            }
            controlTargetRegistrationAuthority.validateMutation(prepared, target, mutation);
            return null;
        } catch (IllegalArgumentException exception) {
            // An explicitly observed registration/binding mismatch is an
            // authoritative position-level rejection. Do not widen this
            // catch to RuntimeException: an Oxia lookup/validation failure is
            // an unproven authority boundary and must retain the Source
            // Position for retry instead of being misreported as
            // UNAUTHORIZED_SYSTEM_MUTATION.
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
    }

    private static boolean requiresControlTargetRegistration(final SystemMutationType type) {
        return type == SystemMutationType.APPLY_SHARD_CONTROL
                || type == SystemMutationType.REPLAY_DEAD_LETTER
                || type == SystemMutationType.RESOLVE_UNCERTAIN;
    }

    private static ControlRef controlRefFor(final SystemMutation mutation) {
        return switch (mutation.type()) {
            case APPLY_SHARD_CONTROL ->
                ApplyShardControlBody.decode(mutation.canonicalBody()).controlRef();
            case REPLAY_DEAD_LETTER ->
                ReplayDeadLetterBody.decode(mutation.canonicalBody()).controlRef();
            case RESOLVE_UNCERTAIN ->
                ResolveUncertainBody.decode(mutation.canonicalBody()).controlRef();
            default -> throw new IllegalArgumentException("mutation has no ControlRef");
        };
    }

    /**
     * Applies the bounded source-ordered Lane PAUSE/RESUME control subset.
     * Break/Close and the shard/profile/grant control branches remain fail-closed
     * until their immutable target registrations and terminal guards are present.
     */
    private SystemMutationResult applyShardControlMutation(
            final SystemMutation mutation, final SourcePosition sourcePosition) {
        final ApplyShardControlBody body = ApplyShardControlBody.decode(mutation.canonicalBody());
        if (!Bytes.constantTimeEquals(
                mutation.logicalOperationIdentity(), body.controlRef().logicalOperationIdentity(body.controlKind()))) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        if (body.controlKind() == 12 || body.controlKind() == 13) {
            return applyPayloadProofTrustSetControlMutation(body, mutation, sourcePosition);
        }
        if (body.controlKind() == 1) {
            return applyProtocolVersionActivationControlMutation(body, mutation, sourcePosition);
        }
        if (body.controlKind() == 2 || body.controlKind() == 3) {
            return applyProfileBindingControlMutation(body, mutation, sourcePosition);
        }
        if (body.controlKind() == 14) {
            return applyInitialRouteControlMutation(body, mutation, sourcePosition);
        }
        if (body.controlKind() < 8 || body.controlKind() > 11) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
        final ApplyShardControlBody.LaneTarget target = body.laneTarget();
        if (body.expectedPriorControlVersion() != null
                && body.expectedPriorControlVersion() != target.expectedControlVersion()) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final LaneRecord current = readLane(target.laneId());
        if (current == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.INTEGRITY_ERROR);
        }
        if (!Arrays.equals(current.laneIncarnation(), target.laneIncarnation())) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.RESOURCE_INCARNATION_MISMATCH);
        }
        if (current.laneControlVersion() != target.expectedControlVersion()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.VERSION_CONFLICT);
        }

        // Resume has a closed result union in the contract. Do not let
        // LaneRecord.resumeByAdmin() collapse an already terminal or
        // source-closed Lane into the generic TOO_LATE result: callers must be
        // able to distinguish an idempotent OPEN, a reversible pause, a
        // source-ordered close, and an irreversible terminal guard.
        if (body.controlKind() == 9) {
            final StableCode resumeCode =
                    switch (current.admissionGate()) {
                        case OPEN -> StableCode.ALREADY_OPEN;
                        case ORDERING_BROKEN -> StableCode.ORDERING_DOMAIN_BROKEN;
                        case CLOSED -> StableCode.LANE_CLOSED;
                        case RETIRED -> StableCode.LANE_TERMINALLY_CLOSED;
                        case ADMIN_PAUSED -> null;
                        case ABSENT -> StableCode.INTEGRITY_ERROR;
                    };
            if (resumeCode != null) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, resumeCode);
            }
        }

        if (body.controlKind() == 10 && (!body.hasAcknowledgement(1) || !body.hasAcknowledgement(3))) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        if (body.controlKind() == 11) {
            final boolean orderedWork;
            try {
                orderedWork = laneHasOrderedWork(target.laneId());
            } catch (IllegalStateException exception) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.INTEGRITY_ERROR);
            }
            if (orderedWork
                    && (!body.allowOrderBreak() || !body.hasAcknowledgement(1) || !body.hasAcknowledgement(3))) {
                return persistSystemResult(
                        mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
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
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.INTEGRITY_ERROR);
        }
        final CloseAccounting closeAccounting;
        final LaneCloseMaterializationCursor closeCursor;
        try {
            if (body.controlKind() == 11) {
                closeAccounting = prepareCloseAccounting(target.laneId(), rollbacks);
                closeCursor = new LaneCloseMaterializationCursor(
                        target.laneId(),
                        next.laneIncarnation(),
                        next.laneControlVersion(),
                        sourcePosition.canonicalBytes(),
                        LaneCloseMaterializationCursor.Phase.MESSAGES,
                        null,
                        closeAccounting.pendingMessages(),
                        closeAccounting.pendingBytes(),
                        closeAccounting.reservationMessages(),
                        closeAccounting.reservationBytes());
            } else {
                closeAccounting = CloseAccounting.empty();
                closeCursor = null;
            }
        } catch (IllegalStateException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.INTEGRITY_ERROR);
        }
        final ShardQuota nextQuota;
        try {
            nextQuota = body.controlKind() == 11
                    ? quota.removeSchedules(closeAccounting.pendingMessages(), closeAccounting.pendingBytes())
                            .removeReservations(
                                    closeAccounting.reservationMessages(), closeAccounting.reservationBytes())
                    : quota;
        } catch (IllegalStateException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.INTEGRITY_ERROR);
        }
        final TimelineCandidate candidate =
                body.controlKind() == 9 ? findLaneCandidate(target.laneId(), null, -1, null, null) : null;
        LaneQuotaUsageProjection nextLaneQuota = body.controlKind() == 11
                        && (closeAccounting.pendingMessages() != 0
                                || closeAccounting.pendingBytes() != 0
                                || closeAccounting.reservationMessages() != 0
                                || closeAccounting.reservationBytes() != 0)
                ? laneQuotaUsage.removeClosedWork(
                        target.laneId(),
                        current.laneIncarnation(),
                        closeAccounting.pendingMessages(),
                        closeAccounting.pendingBytes(),
                        closeAccounting.reservationMessages(),
                        closeAccounting.reservationBytes(),
                        Math.max(1, nextQuota.usageRevision()))
                : laneQuotaUsage;
        for (LaneClaimRollback rollback : rollbacks) {
            nextLaneQuota = mutateInflightQuotaUsage(
                    nextLaneQuota,
                    rollback.claim().laneId(),
                    rollback.claim().laneIncarnation(),
                    claimCharge(rollback.claim()),
                    false,
                    Math.max(1, nextQuota.usageRevision()));
        }
        final LaneQuotaUsageProjection projectedLaneQuota = nextLaneQuota;
        final LaneProjection projection = projectLane(target.laneId(), current, next, candidate, projectedLaneQuota);
        final SystemMutationResult result = SystemMutationResult.from(
                mutation, ApplyStatus.APPLIED, StableCode.OK, sourcePosition.canonicalBytes());
        store.write(batch -> {
            deleteReadyKey(batch, current);
            for (LaneClaimRollback rollback : rollbacks) {
                batch.delete(ColumnFamily.INFLIGHT, rollback.claim().encodedKey());
                batch.putValue(
                        ColumnFamily.ID,
                        1,
                        KeyCodec.idMessage(rollback.claim().delayMessageId()),
                        rollback.nextMessage().encode());
                batch.putValue(
                        ColumnFamily.TIMELINE,
                        1,
                        rollback.claim().timelineKey(),
                        encodeTimelineValue(rollback.claim().delayMessageId(), rollback.nextMessage()));
                batch.putValue(
                        ColumnFamily.TIMELINE,
                        1,
                        expiryKey(rollback.claim().delayMessageId(), rollback.nextMessage()),
                        encodeTimelineValue(rollback.claim().delayMessageId(), rollback.nextMessage()));
            }
            putReadyProjection(batch, projection);
            if (closeCursor != null) {
                batch.putValue(
                        ColumnFamily.TIMELINE,
                        LaneCloseMaterializationCursor.VALUE_TYPE,
                        closeCursorKey(closeCursor),
                        closeCursor.canonicalBytes());
            }
            persistQuota(batch, nextQuota, projectedLaneQuota);
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        quota = nextQuota;
        laneQuotaUsage = projectedLaneQuota;
        return result;
    }

    /**
     * Applies one source-ordered Protocol Version marker. The current
     * CompatibleControlSnapshot is the local proof that the eligible-reader
     * set contains the tuple; the marker carries the immutable external
     * reader-evidence digest. Both are retained so a later command can never
     * silently use a tuple before its marker.
     */
    private SystemMutationResult applyProtocolVersionActivationControlMutation(
            final ApplyShardControlBody body, final SystemMutation mutation, final SourcePosition sourcePosition) {
        final CompatibleControlSnapshot snapshot = store.controlSnapshot();
        if (snapshot == null) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.ROUTE_SNAPSHOT_UNAVAILABLE);
        }
        final var payload = body.protocolVersionActivate();
        if (!snapshot.protocolTuples().contains(payload.tuple())) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        if (payload.isCurrentGeneration()) {
            if (dataResetActivationGate == null) {
                return persistSystemResult(
                        mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
            }
            try {
                dataResetActivationGate.requireSourceApply(
                        payload.tuple(),
                        payload.artifactGenerationSet(),
                        payload.manifestDigest(),
                        sourcePosition.brokerPersistenceTimeEpochMs());
            } catch (IllegalArgumentException | IllegalStateException rejected) {
                return persistSystemResult(
                        mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
            }
        }
        final ProtocolActivationState current = protocolActivationState;
        if (current != null) {
            final ProtocolActivationState.Activation existing = current.activation(payload.tuple());
            if (existing != null) {
                if (!Arrays.equals(existing.canonicalSchemaHash(), payload.canonicalSchemaHash())
                        || !Arrays.equals(
                                existing.compatibleReaderSetEvidenceHash(),
                                payload.compatibleReaderSetEvidenceHash())) {
                    return persistSystemResult(
                            mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
                }
                if (existing.isCurrentGeneration() != payload.isCurrentGeneration()
                        || (payload.isCurrentGeneration()
                                && (!existing.artifactGenerationSet().equals(payload.artifactGenerationSet())
                                        || !Arrays.equals(existing.manifestDigest(), payload.manifestDigest())))) {
                    return persistSystemResult(
                            mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
                }
                return persistSystemResult(
                        mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
            }
        }
        final ProtocolActivationState base =
                current == null ? new ProtocolActivationState(new ShardSubject(store.shardId()), List.of()) : current;
        final ProtocolActivationState next = payload.isCurrentGeneration()
                ? base.activate(
                        payload.tuple(),
                        payload.canonicalSchemaHash(),
                        payload.compatibleReaderSetEvidenceHash(),
                        payload.artifactGenerationSet(),
                        payload.manifestDigest(),
                        sourcePosition,
                        mutation.systemMutationId())
                : base.activate(
                        payload.tuple(),
                        payload.canonicalSchemaHash(),
                        payload.compatibleReaderSetEvidenceHash(),
                        sourcePosition,
                        mutation.systemMutationId());
        final SystemMutationResult result = SystemMutationResult.from(
                mutation, ApplyStatus.APPLIED, StableCode.OK, sourcePosition.canonicalBytes());
        store.write(batch -> {
            batch.putValue(
                    ColumnFamily.META,
                    PROTOCOL_ACTIVATION_VALUE_TYPE,
                    KeyCodec.metaFixed(META_PROTOCOL_ACTIVATION_STATE),
                    next.canonicalBytes());
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        protocolActivationState = next;
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        return result;
    }

    /**
     * Managed Command remains the initial compatibility baseline. Once a
     * kind-1 marker has been observed, every other tuple must be present in
     * the authenticated control snapshot and in the durable marker set.
     */
    private StableCode validateCommandProtocolTuple(final PreparedCommand command) {
        if (protocolActivationState == null) {
            return null;
        }
        final CompatibleControlSnapshot snapshot = store.controlSnapshot();
        if (snapshot == null) {
            return StableCode.ROUTE_SNAPSHOT_UNAVAILABLE;
        }
        final ProtocolTuple tuple = command.protocolTuple();
        if (protocolActivationState.isMarkedActivated(tuple)) {
            return snapshot.protocolTuples().contains(tuple) ? null : StableCode.UNSUPPORTED_ACTIVATED_PROTOCOL;
        }
        if (ProtocolTuple.managedCommand().equals(tuple)
                && snapshot.protocolTuples().contains(tuple)) {
            return null;
        }
        return StableCode.UNACTIVATED_PROTOCOL_VERSION;
    }

    /**
     * Applies the one-time source-ordered Route control snapshot activation.
     * The payload is deliberately projected into the existing shard-bound
     * {@link CompatibleControlSnapshot}; no second metadata namespace is
     * allowed to represent the same activation input. The snapshot, mutation
     * result and source cursor share one synchronous WriteBatch.
     */
    private SystemMutationResult applyInitialRouteControlMutation(
            final ApplyShardControlBody body, final SystemMutation mutation, final SourcePosition sourcePosition) {
        final InitialRouteControlActivatePayload payload = body.initialRouteControlActivate();
        final CompatibleControlSnapshot snapshot;
        try {
            snapshot = new CompatibleControlSnapshot(
                    new ShardSubject(store.shardId()),
                    payload.protocolTuples(),
                    payload.profiles(),
                    payload.initialQuotaGrant());
        } catch (IllegalArgumentException exception) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        if (!Bytes.constantTimeEquals(snapshot.snapshotDigest(), payload.initialControlSnapshotHash())) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final CompatibleControlSnapshot existing = store.controlSnapshot();
        if (existing != null) {
            if (existing.equals(snapshot)) {
                return persistSystemResult(
                        mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
            }
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final SystemMutationResult result = SystemMutationResult.from(
                mutation, ApplyStatus.APPLIED, StableCode.OK, sourcePosition.canonicalBytes());
        final ProtocolActivationState initialProtocolActivationState =
                new ProtocolActivationState(new ShardSubject(store.shardId()), List.of());
        store.write(batch -> {
            batch.putControlSnapshot(snapshot);
            batch.putValue(
                    ColumnFamily.META,
                    PROTOCOL_ACTIVATION_VALUE_TYPE,
                    KeyCodec.metaFixed(META_PROTOCOL_ACTIVATION_STATE),
                    initialProtocolActivationState.canonicalBytes());
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        protocolActivationState = initialProtocolActivationState;
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        return result;
    }

    /**
     * Applies the source-ordered trust-set marker subset and persists the
     * marker state in the same batch as the mutation result and source cursor.
     * The immutable semantic value is resolved before the batch; this local
     * path does not claim Oxia/catalog durability for that authority.
     */
    private SystemMutationResult applyPayloadProofTrustSetControlMutation(
            final ApplyShardControlBody body, final SystemMutation mutation, final SourcePosition sourcePosition) {
        final PayloadProofTrustSetRef trustSet;
        final PayloadProofTrustSetControlState next;
        if (payloadProofTrustSetControlCatalog == null) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.ROUTE_SNAPSHOT_UNAVAILABLE);
        }
        if (body.controlKind() == 12) {
            final var payload = body.payloadProofTrustSetActivate();
            trustSet = payload.trustSet();
            requireTrustSetSemantic(trustSet);
            if (body.semanticVersion() != trustSet.version()
                    || !Bytes.constantTimeEquals(body.semanticHash(), trustSet.semanticHash())) {
                return persistSystemResult(
                        mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
            }
            next = payloadProofTrustSetControlState.activate(trustSet, sourcePosition);
        } else {
            final var payload = body.payloadProofIssuanceClose();
            trustSet = payload.trustSet();
            final PayloadProofTrustSetSemantic semantic = requireTrustSetSemantic(trustSet);
            if (semantic.keys().stream().noneMatch(key -> key.keyVersion() == payload.proofKeyVersion())) {
                return persistSystemResult(
                        mutation,
                        sourcePosition,
                        ApplyStatus.REJECTED,
                        StableCode.PAYLOAD_PROOF_KEY_NOT_AUTHORIZED_AT_SOURCE_POSITION);
            }
            if (body.semanticVersion() != trustSet.version()
                    || !Bytes.constantTimeEquals(body.semanticHash(), trustSet.semanticHash())) {
                return persistSystemResult(
                        mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
            }
            next = payloadProofTrustSetControlState.close(payload, sourcePosition);
        }
        final SystemMutationResult result = SystemMutationResult.from(
                mutation, ApplyStatus.APPLIED, StableCode.OK, sourcePosition.canonicalBytes());
        store.write(batch -> {
            batch.putValue(
                    ColumnFamily.META,
                    PAYLOAD_PROOF_CONTROL_VALUE_TYPE,
                    KeyCodec.metaFixed(META_PAYLOAD_PROOF_CONTROL_STATE),
                    next.canonicalBytes());
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        payloadProofTrustSetControlState = next;
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        return result;
    }

    private PayloadProofTrustSetSemantic requireTrustSetSemantic(final PayloadProofTrustSetRef reference) {
        final PayloadProofTrustSetSemantic semantic = payloadProofTrustSetControlCatalog.resolve(reference);
        if (semantic == null || !semantic.ref().equals(reference)) {
            throw new CommandResolutionException(
                    StableCode.ROUTE_SNAPSHOT_UNAVAILABLE,
                    "trust-set semantic value is unavailable or does not match its reference");
        }
        return semantic;
    }

    private SystemMutationResult applyProfileBindingControlMutation(
            final ApplyShardControlBody body, final SystemMutation mutation, final SourcePosition sourcePosition) {
        final ProfileRef profile;
        final ProfileBindingControlState next;
        if (body.controlKind() == 2) {
            final ProfileBindingActivatePayload payload = body.profileBindingActivate();
            profile = payload.profile();
            if (!profileReferenceMatchesBody(body, profile)) {
                return persistSystemResult(
                        mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
            }
            next = profileBindingControlState.activate(profile, sourcePosition);
        } else {
            final ProfileNewBindingClosePayload payload = body.profileNewBindingClose();
            profile = payload.profile();
            if (!profileReferenceMatchesBody(body, profile)) {
                return persistSystemResult(
                        mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
            }
            next = profileBindingControlState.close(payload, sourcePosition);
        }
        final SystemMutationResult result = SystemMutationResult.from(
                mutation, ApplyStatus.APPLIED, StableCode.OK, sourcePosition.canonicalBytes());
        store.write(batch -> {
            batch.putValue(
                    ColumnFamily.META,
                    PROFILE_CONTROL_VALUE_TYPE,
                    KeyCodec.metaFixed(META_PROFILE_CONTROL_STATE),
                    next.canonicalBytes());
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        profileBindingControlState = next;
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        return result;
    }

    private static boolean profileReferenceMatchesBody(final ApplyShardControlBody body, final ProfileRef profile) {
        return body.semanticVersion() == profile.version()
                && Bytes.constantTimeEquals(body.semanticHash(), profile.semanticHash());
    }

    private void requireProfileFirstBinding(final ProfileRef profile, final SourcePosition sourcePosition) {
        if (!profileBindingControlState.hasMarkers()) {
            // A catalog-backed path is the production-shaped seam: it must
            // not infer that an empty marker projection means "all Profiles
            // are active". The activation target set is source ordered and a
            // new Route cannot admit tenant bindings until its first marker
            // has been durably applied. Constructors without a Profile
            // catalog remain the explicitly bounded legacy compatibility path.
            if (profileCatalog == null) {
                return;
            }
            throw new CommandResolutionException(
                    StableCode.PROFILE_VERSION_NOT_ACTIVE_AT_SOURCE_POSITION,
                    "Profile activation markers are not applied for this Route");
        }
        final ProfileAcceptance acceptance = profileBindingControlState.firstBindingAcceptance(profile, sourcePosition);
        if (acceptance == ProfileAcceptance.ABSENT) {
            throw new CommandResolutionException(
                    StableCode.PROFILE_VERSION_NOT_ACTIVE_AT_SOURCE_POSITION,
                    "Profile version is not active for first binding at this source position");
        }
        if (acceptance == ProfileAcceptance.CLOSED_FOR_FIRST_BINDING) {
            throw new CommandResolutionException(
                    StableCode.PROFILE_DEPRECATED_FOR_NEW_USE,
                    "Profile version is closed for first binding at this source position");
        }
    }

    /** Builds the exact reversible timeline projection that Pause must restore in its own batch. */
    private List<LaneClaimRollback> prepareLaneClaimRollbacks(
            final com.nereusstream.delay.protocol.DestinationLaneId laneId) {
        final int limit = boundedLimitPlusOne(config.maxPendingMessages());
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> entries = store.scan(
                ColumnFamily.INFLIGHT,
                new byte[] {INFLIGHT_CLAIMED_KIND, 1},
                new byte[] {INFLIGHT_PUBLISHING_KIND, 1},
                limit);
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
            if (current == null
                    || current.status() != MessageStatus.CLAIMED
                    || current.generation() != claim.generation()
                    || current.stateVersion() != claim.runtimeRevision()) {
                throw new IllegalStateException("Claim does not match current message during lane Pause");
            }
            final ClaimResultBody.ClaimPrecondition precondition =
                    ClaimResultBody.decodePrecondition(claim.preconditionBytes());
            final TimelineWorkKind workKind = TimelineWorkKind.fromWire(precondition.sourceWorkKind());
            final MessageRecord next = restoreClaimedMessageToTimeline(claim, current, workKind);
            if (!Arrays.equals(claim.timelineKey(), timelineKey(claim.delayMessageId(), next))) {
                throw new IllegalStateException("Claim timeline key is not reversible");
            }
            result.add(new LaneClaimRollback(claim, next));
        }
        return List.copyOf(result);
    }

    /**
     * Computes the one-time quota transfer owned by a Close marker. The scan
     * is deliberately bounded by the shard's hard pending-message limit: if a
     * complete proof cannot be made, the source mutation is rejected rather
     * than installing a close overlay with guessed counters.
     */
    private CloseAccounting prepareCloseAccounting(
            final com.nereusstream.delay.protocol.DestinationLaneId laneId, final List<LaneClaimRollback> rollbacks) {
        final int limit = boundedLimitPlusOne(config.maxPendingMessages());
        final Map<DelayMessageId, MessageRecord> rollbackMessages = new HashMap<>();
        for (LaneClaimRollback rollback : rollbacks) {
            rollbackMessages.put(rollback.claim().delayMessageId(), rollback.nextMessage());
        }
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> messages =
                store.scan(ColumnFamily.ID, new byte[] {1, 1}, new byte[] {2, 1}, limit);
        if (messages.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("message scan exceeded configured bound during lane close");
        }
        long pendingMessages = 0;
        long pendingBytes = 0;
        for (var entry : messages) {
            if (isRetiredMessageEntry(entry, "lane close accounting")) {
                continue;
            }
            final MessageRecord stored = decodeMessageEntry(entry, "lane close accounting");
            final MessageRecord message = rollbackMessages.getOrDefault(messageIdFromEntry(entry), stored);
            if (message.laneId().equals(laneId) && isUnadmittedGeneration(message)) {
                pendingMessages = Math.addExact(pendingMessages, 1);
                pendingBytes = Math.addExact(pendingBytes, message.payloadLength());
            }
        }
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> reservations =
                store.scan(ColumnFamily.ID, new byte[] {2, 1}, new byte[] {3, 1}, limit);
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
                reservationBytes =
                        Math.addExact(reservationBytes, reservation.intent().expectedPayloadLength());
            }
        }
        return new CloseAccounting(pendingMessages, pendingBytes, reservationMessages, reservationBytes);
    }

    private static boolean isUnadmittedGeneration(final MessageRecord message) {
        return (message.status() == MessageStatus.SCHEDULED || message.status() == MessageStatus.CLAIMED)
                && message.runtimeIndex().attemptObligations().isEmpty();
    }

    private MessageRecord decodeMessageEntry(
            final com.nereusstream.delay.store.ShardStore.KeyValue entry, final String context) {
        if (entry.key().length != 2 + DelayMessageId.LENGTH || entry.key()[0] != 1 || entry.key()[1] != 1) {
            throw new IllegalStateException("invalid MESSAGE key during " + context);
        }
        final DelayMessageId messageId = messageIdFromEntry(entry);
        final ValueEnvelope.Decoded value = ValueEnvelope.decodeAny(entry.value());
        if (value.valueType() != MESSAGE_VALUE_TYPE || RetiredMessageIdentityRecord.isEncoded(value.payload())) {
            throw new IllegalStateException("retired/non-Message value encountered during " + context);
        }
        return validateMessageSourcePosition(messageId, MessageRecord.decode(value.payload()), context);
    }

    /**
     * Validates and recognizes the compact branch while scanning the shared
     * MESSAGE key range. Rebuild/retirement scans must skip a tombstone as a
     * live Message, but malformed branch bytes still fail closed.
     */
    private boolean isRetiredMessageEntry(
            final com.nereusstream.delay.store.ShardStore.KeyValue entry, final String context) {
        if (entry.key().length != 2 + DelayMessageId.LENGTH || entry.key()[0] != 1 || entry.key()[1] != 1) {
            throw new IllegalStateException("invalid MESSAGE key during " + context);
        }
        final ValueEnvelope.Decoded value = ValueEnvelope.decodeAny(entry.value());
        if (value.valueType() != MESSAGE_VALUE_TYPE) {
            throw new IllegalStateException("MESSAGE key has an unregistered value type during " + context);
        }
        if (!RetiredMessageIdentityRecord.isEncoded(value.payload())) {
            return false;
        }
        final RetiredMessageIdentityRecord retired = RetiredMessageIdentityRecord.decode(value.payload());
        final DelayMessageId messageId = messageIdFromEntry(entry);
        if (!retired.messageId().equals(messageId)) {
            throw new IllegalStateException("retired Message key/value identity mismatch during " + context);
        }
        validateSourcePositionShard(retired.appliedSourcePosition(), "retired Message " + context);
        return true;
    }

    private static DelayMessageId messageIdFromEntry(final com.nereusstream.delay.store.ShardStore.KeyValue entry) {
        return new DelayMessageId(Arrays.copyOfRange(entry.key(), 2, entry.key().length));
    }

    private PayloadReservation decodeReservationEntry(
            final com.nereusstream.delay.store.ShardStore.KeyValue entry, final String context) {
        if (entry.key().length != 2 + 32 || entry.key()[0] != 2 || entry.key()[1] != 1) {
            throw new IllegalStateException("invalid RESERVATION key during " + context);
        }
        return validateReservationIdentity(
                Arrays.copyOfRange(entry.key(), 2, entry.key().length),
                PayloadReservation.decode(ValueEnvelope.decode(entry.value(), 2).payload()),
                context);
    }

    private PayloadReservation validateReservationIdentity(
            final byte[] reservationId, final PayloadReservation reservation, final String context) {
        if (!Arrays.equals(reservation.reservationId(), reservationId)
                || !reservation.shardId().equals(store.shardId())) {
            throw new IllegalStateException("RESERVATION key/value identity mismatch during " + context);
        }
        validateSourcePositionShard(reservation.sourcePosition(), "RESERVATION " + context);
        return reservation;
    }

    private MessageRecord validateMessageSourcePosition(
            final DelayMessageId messageId, final MessageRecord message, final String context) {
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
        payloadProofTrustSetControlState
                .activations()
                .forEach(marker -> validateSourcePositionShard(
                        marker.sourcePosition().canonicalBytes(), "payload proof trust-set activation state"));
        payloadProofTrustSetControlState
                .closures()
                .forEach(marker -> validateSourcePositionShard(
                        marker.sourcePosition().canonicalBytes(), "payload proof trust-set closure state"));
        profileBindingControlState
                .activations()
                .forEach(marker -> validateSourcePositionShard(
                        marker.sourcePosition().canonicalBytes(), "Profile activation state"));
        profileBindingControlState
                .closures()
                .forEach(marker ->
                        validateSourcePositionShard(marker.sourcePosition().canonicalBytes(), "Profile closure state"));
    }

    private List<ClosedMessageAction> prepareClosedMessageActions(
            final LaneCloseMaterializationCursor cursor,
            final List<com.nereusstream.delay.store.ShardStore.KeyValue> entries,
            final SourcePosition closePosition) {
        final List<ClosedMessageAction> result = new ArrayList<>();
        for (var entry : entries) {
            if (isRetiredMessageEntry(entry, "Lane close materialization")) {
                continue;
            }
            final DelayMessageId messageId = messageIdFromEntry(entry);
            final MessageRecord current = decodeMessageEntry(entry, "Lane close materialization");
            if (!current.laneId().equals(cursor.laneId()) || !isUnadmittedGeneration(current)) {
                continue;
            }
            final ClaimRecord claim = current.status() == MessageStatus.CLAIMED ? findClaimForMessage(messageId) : null;
            if (current.status() == MessageStatus.CLAIMED && claim == null) {
                throw new IllegalStateException("closed CLAIMED message has no durable Claim");
            }
            if (claim != null
                    && (!claim.delayMessageId().equals(messageId)
                            || claim.generation() != current.generation()
                            || claim.runtimeRevision() != current.stateVersion())) {
                throw new IllegalStateException("closed Claim does not match current message");
            }
            final long nextStateVersion = Math.addExact(current.stateVersion(), 1);
            final MessageRecord terminalMessage = MessageRecord.current(
                            MessageStatus.DEAD_LETTER,
                            current.generation(),
                            nextStateVersion,
                            current.deliverAtEpochMs(),
                            current.expireAtEpochMs(),
                            current.laneId(),
                            current.orderingMode(),
                            current.payload(),
                            current.scheduleSourcePosition(),
                            current.payloadReference(),
                            current.retryEligibilityAtEpochMs())
                    .withRuntimeIndex(GenerationRuntimeIndex.none(
                            GenerationAggregateState.DEAD_LETTER,
                            List.of(),
                            current.runtimeIndex().admissionsUsed(),
                            current.runtimeIndex().uncertainRetryAdmissionsUsed(),
                            false,
                            nextStateVersion));
            final TerminalGenerationRecord terminal = new TerminalGenerationRecord(
                    messageId,
                    current.generation(),
                    MessageStatus.DEAD_LETTER,
                    StableCode.LANE_CLOSED_BEFORE_ADMISSION,
                    nextStateVersion,
                    closePosition.canonicalBytes(),
                    false,
                    List.of());
            result.add(new ClosedMessageAction(
                    messageId,
                    current,
                    claim,
                    claim == null ? timelineKey(messageId, current) : claim.timelineKey(),
                    expiryKey(messageId, current),
                    terminalMessage,
                    terminal));
        }
        return List.copyOf(result);
    }

    private List<ClosedReservationAction> prepareClosedReservationActions(
            final LaneCloseMaterializationCursor cursor,
            final List<com.nereusstream.delay.store.ShardStore.KeyValue> entries,
            final SourcePosition closePosition) {
        final List<ClosedReservationAction> result = new ArrayList<>();
        for (var entry : entries) {
            final PayloadReservation reservation = decodeReservationEntry(entry, "Lane close materialization");
            if (!reservation.intent().laneId().equals(cursor.laneId())
                    || reservation.status() != PayloadReservationStatus.RESERVED) {
                continue;
            }
            final PayloadReservation closed = reservation.withLifecycle(
                    PayloadReservationStatus.ABANDONED,
                    Math.addExact(reservation.stateVersion(), 1),
                    closePosition.canonicalBytes(),
                    null);
            result.add(new ClosedReservationAction(reservation, closed));
        }
        return List.copyOf(result);
    }

    private CursorScan scanAfter(
            final ColumnFamily family,
            final byte[] lowerInclusive,
            final byte[] upperExclusive,
            final byte[] lastKey,
            final int limit) {
        final int requestLimit = limit == Integer.MAX_VALUE ? limit : Math.addExact(limit, 1);
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> scanned =
                store.scan(family, lastKey.length == 0 ? lowerInclusive : lastKey, upperExclusive, requestLimit);
        int start = 0;
        if (lastKey.length != 0
                && !scanned.isEmpty()
                && Arrays.equals(scanned.get(0).key(), lastKey)) {
            start = 1;
        }
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> after = scanned.subList(start, scanned.size());
        final boolean more = after.size() > limit;
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> entries = more ? after.subList(0, limit) : after;
        return new CursorScan(List.copyOf(entries), more);
    }

    private static byte[] closeCursorKey(final LaneCloseMaterializationCursor cursor) {
        return closeCursorKey(cursor.laneId(), cursor.closeVersion());
    }

    private static byte[] closeCursorKey(
            final com.nereusstream.delay.protocol.DestinationLaneId laneId, final long closeVersion) {
        return KeyCodec.timelineSystem(
                LaneCloseMaterializationCursor.SYSTEM_WORK_KIND, 0, laneId.bytes(), closeVersion);
    }

    private static SystemTimelineKey decodeLaneCloseWorkKey(final byte[] key) {
        Objects.requireNonNull(key, "system work key");
        if (key.length < 3 + Long.BYTES + Integer.BYTES + Long.BYTES
                || key[0] != 6
                || key[1] != 1
                || key[2] != LaneCloseMaterializationCursor.SYSTEM_WORK_KIND) {
            throw new IllegalStateException("invalid Lane close system work key");
        }
        final ByteBuffer input = ByteBuffer.wrap(key);
        input.position(3);
        final long nextEligibleAt = input.getLong();
        final int workIdLength = input.getInt();
        if (nextEligibleAt < 0
                || workIdLength != DestinationLaneId.LENGTH
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
    private boolean laneHasOrderedWork(final com.nereusstream.delay.protocol.DestinationLaneId laneId) {
        final int limit = boundedLimitPlusOne(config.maxPendingMessages());
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> entries =
                store.scan(ColumnFamily.ID, new byte[] {1, 1}, new byte[] {2, 1}, limit);
        if (entries.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("message scan exceeded configured bound during lane Close");
        }
        for (var entry : entries) {
            if (entry.key().length != 2 + DelayMessageId.LENGTH || entry.key()[0] != 1 || entry.key()[1] != 1) {
                throw new IllegalStateException("invalid MESSAGE key during lane Close");
            }
            if (isRetiredMessageEntry(entry, "lane Close")) {
                continue;
            }
            final MessageRecord message = decodeMessageEntry(entry, "lane Close");
            if (message.laneId().equals(laneId)
                    && !isTerminalStatus(message.status())
                    && message.orderingMode() == com.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO) {
                return true;
            }
        }
        return false;
    }

    private SystemMutationResult applyPublishAdmissionMutation(
            final SystemMutation mutation, final SourcePosition sourcePosition) {
        final PublishAdmissionBody body = PublishAdmissionBody.decode(mutation.canonicalBody());
        if (!Bytes.constantTimeEquals(mutation.logicalOperationIdentity(), body.publishAttemptId())) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final com.nereusstream.delay.protocol.AuthorIdentity author =
                com.nereusstream.delay.protocol.AuthorIdentity.decode(mutation.authorIdentity());
        if (!Arrays.equals(body.ownerIdentity(), author.asOwnerIdentity().canonicalBytes())) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        try {
            validatePublishAdmissionTiming(body);
            body.requireTiming(
                    body.descriptor().actionAtEpochMs(), body.descriptor().expireAtEpochMs());
            body.requireBrokerTiming(
                    sourcePosition.brokerPersistenceTimeEpochMs(),
                    config.maxIngressBrokerTimestampDivergenceMs(),
                    config.maximumAdmissionMutationEnqueueAgeMs());
        } catch (IllegalArgumentException timingFailure) {
            revokeMatchingAdmissionClaim(body, author);
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.STALE_SYSTEM_MUTATION);
        }
        final DelayMessageId messageId = new DelayMessageId(body.messageId());
        final com.nereusstream.delay.protocol.DestinationLaneId laneId =
                new com.nereusstream.delay.protocol.DestinationLaneId(body.laneId());
        final PublishAttemptLedger open = findOpenPublishAttempt(body.publishAttemptId());
        if (open != null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
        final MessageRecord current = getMessage(messageId);
        if (current == null
                || (current.status() != MessageStatus.SCHEDULED && current.status() != MessageStatus.CLAIMED)
                || current.generation() != body.generation()
                || !current.laneId().equals(laneId)
                || current.deliverAtEpochMs() != body.descriptor().deliverAtEpochMs()
                || current.expireAtEpochMs() != body.descriptor().expireAtEpochMs()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
        final LaneRecord lane = readLane(laneId);
        if (lane == null || !lane.schedulable()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
        final ClaimRecord localClaim =
                current.status() == MessageStatus.CLAIMED ? getClaim(body.claimId(), author.generation()) : null;
        final AdmissionReplayState replayState;
        try {
            replayState = validatePublishAdmissionReplayState(body, current, lane, localClaim, sourcePosition);
        } catch (IllegalArgumentException | IllegalStateException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
        final SloSampleStart dueAdmissionStart = dueAdmissionStart(body);
        final OutcomeReserveUsage admissionCharge;
        try {
            admissionCharge = OutcomeReserveUsage.from(body.chargeVector());
        } catch (ArithmeticException | IllegalArgumentException overflow) {
            return persistAdmissionCapacityGated(body, mutation, sourcePosition, localClaim, dueAdmissionStart);
        }
        if (!outcomeReserve.fits(admissionCharge, config.maxOutcomeReserveRecords(), config.maxOutcomeReserveBytes())) {
            return persistAdmissionCapacityGated(body, mutation, sourcePosition, localClaim, dueAdmissionStart);
        }
        try {
            validateOutcomeReserveVector(
                    outcomeReserveVector.add(body.chargeVector().toCapacityVector()));
        } catch (ArithmeticException | IllegalArgumentException | IllegalStateException exception) {
            return persistAdmissionCapacityGated(body, mutation, sourcePosition, localClaim, dueAdmissionStart);
        }
        if (current.status() == MessageStatus.CLAIMED) {
            if (localClaim != null
                    && (!localClaim.delayMessageId().equals(messageId)
                            || localClaim.generation() != body.generation()
                            || !Arrays.equals(
                                    localClaim.preconditionBytes(),
                                    body.claimPrecondition().canonicalBytes()))) {
                return persistSystemResult(
                        mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
            }
        }
        final long firstAttemptAt = body.decisionTime().latestEpochMs();
        final RetryPolicySemantic admissionPolicy = retryPolicyFor(messageId, current, sourcePosition);
        final long retryDeadline =
                retryDeadlineForAdmission(firstAttemptAt, current.expireAtEpochMs(), admissionPolicy);
        final PublishAttemptLedger admission = PublishAttemptLedger.publishingWithRetryWindow(
                messageId,
                body.generation(),
                body.publishAttemptId(),
                body.claimId(),
                author.generation(),
                body.descriptor().attemptNo(),
                laneId,
                body.laneIncarnation(),
                body.ownerIdentity(),
                body.storeIncarnation(),
                body.preparedPublishHash(),
                mutation.canonicalBody(),
                firstAttemptAt,
                retryDeadline,
                sourcePosition.canonicalBytes());
        final SystemMutationResult result = SystemMutationResult.from(
                mutation, ApplyStatus.APPLIED, StableCode.OK, sourcePosition.canonicalBytes());
        try {
            admitPublishAttempt(
                    admission,
                    sourcePosition,
                    result,
                    replayState.claimMayBeMissing(),
                    replayState.uncertainRetryAdmission(),
                    admissionCharge,
                    dueAdmissionStart);
            return result;
        } catch (ShardStore.RocksDbWriteFailure exception) {
            // A native WriteBatch failure is not semantic staleness. Let the
            // caller stop before Source ACK instead of trying to persist a
            // second result that would incorrectly advance the source cursor.
            throw exception;
        } catch (SloStartMaterializationException exception) {
            throw exception;
        } catch (IllegalArgumentException | IllegalStateException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
    }

    private void revokeMatchingAdmissionClaim(
            final PublishAdmissionBody body, final com.nereusstream.delay.protocol.AuthorIdentity author) {
        final ClaimRecord claim = getClaim(body.claimId(), author.generation());
        if (claim == null
                || !Arrays.equals(
                        claim.preconditionBytes(), body.claimPrecondition().canonicalBytes())) {
            return;
        }
        final MessageRecord current = getMessage(new DelayMessageId(body.messageId()));
        if (current == null
                || current.status() != MessageStatus.CLAIMED
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
        final ProfileRef destinationRef = ProfileRef.decode(body.descriptor().destinationProfile());
        final ProfileRef capabilityRef = ProfileRef.decode(body.descriptor().capabilityProfile());
        final ProfileSemanticEnvelope destination = profileCatalog.resolve(destinationRef);
        final ProfileSemanticEnvelope capability = profileCatalog.resolve(capabilityRef);
        if (destination == null
                || capability == null
                || !destination.ref().equals(destinationRef)
                || !capability.ref().equals(capabilityRef)
                || !(destination.body() instanceof DestinationProfileSemantic destinationBody)
                || !(capability.body() instanceof DeliveryCapabilitySemantic capabilityBody)) {
            throw new IllegalArgumentException("Publish Admission Profile semantics are unavailable");
        }
        body.requireTimingPolicy(destinationBody, capabilityBody);
    }

    /**
     * Advances a source-ordered Admission that cannot fit its shard outcome
     * reserve. A live Claim is revoked in the same batch; no attempt or
     * Producer-side state is created.
     */
    private SystemMutationResult persistAdmissionCapacityGated(
            final PublishAdmissionBody body,
            final SystemMutation mutation,
            final SourcePosition sourcePosition,
            final ClaimRecord claim,
            final SloSampleStart dueAdmissionStart) {
        final DelayMessageId messageId = new DelayMessageId(body.messageId());
        final MessageRecord current = getMessage(messageId);
        if (current == null
                || (current.status() != MessageStatus.SCHEDULED && current.status() != MessageStatus.CLAIMED)) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
        MessageRecord next = current;
        final boolean revokeClaim = current.status() == MessageStatus.CLAIMED;
        final byte[] priorTimelineKey = claim == null ? timelineKey(messageId, current) : claim.timelineKey();
        if (revokeClaim) {
            final ClaimResultBody.ClaimPrecondition precondition =
                    ClaimResultBody.decodePrecondition(body.claimPrecondition().canonicalBytes());
            final TimelineWorkKind workKind = TimelineWorkKind.fromWire(precondition.sourceWorkKind());
            if (claim != null) {
                next = restoreClaimedMessageToTimeline(claim, current, workKind);
            } else {
                next = MessageRecord.current(
                        MessageStatus.SCHEDULED,
                        current.generation(),
                        Math.addExact(current.stateVersion(), 1),
                        current.deliverAtEpochMs(),
                        current.expireAtEpochMs(),
                        current.laneId(),
                        current.orderingMode(),
                        current.payload(),
                        current.scheduleSourcePosition(),
                        current.payloadReference(),
                        current.retryEligibilityAtEpochMs());
                next = next.withRuntimeIndex(timelineRuntimeIndex(
                        messageId,
                        next,
                        workKind,
                        Math.addExact(current.runtimeIndex().admissionsUsed(), 1),
                        next.stateVersion(),
                        UncertainRetryAuthority.NONE,
                        null,
                        null,
                        current.runtimeIndex()));
            }
        }
        final LaneQuotaUsageProjection nextLaneQuota =
                revokeClaim && claim != null ? removeClaimQuotaUsage(claim) : laneQuotaUsage;
        final MessageRecord nextForWrite = next;
        final Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections = revokeClaim
                ? readyProjections(sourcePosition, messageId, current, next, null, nextLaneQuota)
                : Map.of();
        final SystemMutationResult result = SystemMutationResult.from(
                mutation, ApplyStatus.APPLIED, StableCode.ADMISSION_CAPACITY_GATED, sourcePosition.canonicalBytes());
        store.write(batch -> {
            if (revokeClaim) {
                batch.delete(
                        ColumnFamily.INFLIGHT,
                        claim == null
                                ? KeyCodec.inflight(
                                        INFLIGHT_CLAIMED_KIND,
                                        OwnerIdentity.decode(body.ownerIdentity())
                                                .ownerEpoch(),
                                        body.claimId())
                                : claim.encodedKey());
                batch.delete(ColumnFamily.TIMELINE, priorTimelineKey);
                batch.delete(ColumnFamily.TIMELINE, expiryKey(messageId, current));
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(messageId), nextForWrite.encode());
                batch.putValue(
                        ColumnFamily.TIMELINE,
                        1,
                        timelineKey(messageId, nextForWrite),
                        encodeTimelineValue(messageId, nextForWrite));
                batch.putValue(
                        ColumnFamily.TIMELINE,
                        1,
                        expiryKey(messageId, nextForWrite),
                        encodeTimelineValue(messageId, nextForWrite));
                for (LaneProjection projection : projections.values()) {
                    deleteReadyKey(batch, projection.previousLane());
                    putReadyProjection(batch, projection);
                }
            }
            persistQuota(batch, quota, nextLaneQuota);
            writeSystemResult(batch, result);
            persistSloStart(batch, dueAdmissionStart);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        laneQuotaUsage = nextLaneQuota;
        return result;
    }

    private SystemMutationResult applyTimeFenceMutation(
            final SystemMutation mutation, final SourcePosition sourcePosition) {
        final List<com.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field> fields =
                SystemMutationBodyCodec.fields(SystemMutationType.TIME_FENCE, mutation.canonicalBody());
        final long closeThrough = bodyNonNegative(field(fields, 10), 10);
        final int fenceKeyVersion = bodyUint32Bits(field(fields, 11), 11);
        final byte[] proofId = fixedBodyBytes(field(fields, 12), 12, SystemMutation.HASH_LENGTH);
        final TrustedUtcIntervalEvidence proof = TrustedUtcIntervalEvidence.decode(bytesBody(field(fields, 13), 13));
        final long minimumProofEarliest;
        try {
            minimumProofEarliest = Math.addExact(closeThrough, config.timeFenceSafetyMarginMs());
        } catch (ArithmeticException overflow) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        if (fenceKeyVersion != mutation.signingKeyVersion() || proof.earliestEpochMs() < minimumProofEarliest) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final byte[] expectedProofId = Bytes.sha256(
                Bytes.utf8("nereus-delay-time-fence-proof\0"),
                store.shardId().routeIncarnation().bytes(),
                Bytes.u32beBits(store.shardId().partition()),
                Bytes.i64be(closeThrough),
                Bytes.u32beBits(fenceKeyVersion),
                Bytes.lp32(proof.canonicalBytes()));
        if (!Bytes.constantTimeEquals(proofId, expectedProofId)
                || !Bytes.constantTimeEquals(mutation.logicalOperationIdentity(), proofId)) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final SystemMutationResult result = SystemMutationResult.from(
                mutation, ApplyStatus.APPLIED, StableCode.OK, sourcePosition.canonicalBytes());
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
        mutationSequence = nextMutationSequence();
        return result;
    }

    private SystemMutationResult applyPublishOutcomeMutation(
            final SystemMutation mutation, final SourcePosition sourcePosition) {
        final List<com.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field> fields =
                SystemMutationBodyCodec.fields(SystemMutationType.PUBLISH_OUTCOME, mutation.canonicalBody());
        final byte[] attemptId = fixedBodyBytes(field(fields, 10), 10, PublishAttemptLedger.HASH_LENGTH);
        final int sideEffect = bodyInt(field(fields, 11), 11);
        final int disposition = bodyInt(field(fields, 12), 12);
        final StableCode code = StableCode.fromWire(bodyInt(field(fields, 13), 13));
        final byte[] evidence = optionalBodyBytes(fields, 14);
        final com.nereusstream.delay.protocol.AuthorIdentity author =
                com.nereusstream.delay.protocol.AuthorIdentity.decode(mutation.authorIdentity());
        final PublishOutcomeBody outcome = PublishOutcomeBody.decode(mutation.canonicalBody());
        if (!Arrays.equals(outcome.publishAttemptId(), attemptId)) {
            throw new IllegalArgumentException("Publish Outcome attempt identity mismatch");
        }
        if (!Bytes.constantTimeEquals(mutation.logicalOperationIdentity(), outcome.initialLogicalOperationIdentity())) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        PublishAttemptLedger ledger = getPublishAttempt(attemptId, author.generation());
        boolean recoveryUnknown = false;
        if (ledger == null) {
            // A different Owner epoch cannot address the durable key directly.
            // Scan only this exact attempt so a mismatched author is reported
            // as an authorization failure instead of being mistaken for a
            // missing/stale attempt. The scan is bounded and fails closed.
            ledger = findOpenPublishAttempt(attemptId);
            recoveryUnknown = sideEffect == 3
                    && isCrossOwnerRecoveryUnknown(outcome)
                    && ledger != null
                    && ledger.state() == AttemptLedgerState.PUBLISHING
                    && ledger.ownerEpoch() != author.generation()
                    && canonicalOwnerIdentity(ledger.ownerIdentity()) != null
                    && canonicalOwnerIdentity(ledger.ownerIdentity()).ownerEpoch() == ledger.ownerEpoch();
        }
        if (ledger == null || ledger.state() != AttemptLedgerState.PUBLISHING) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
        if (!recoveryUnknown && !matchesAdmittedOwner(ledger, author)) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        requirePublishAttemptLane(ledger, "publish outcome");
        final MessageRecord outcomeCurrent = getMessage(ledger.delayMessageId());
        if (outcomeCurrent != null && outcomeCurrent.generation() == ledger.generation()) {
            validateRetryDecisionBinding(outcome, ledger, outcomeCurrent, sourcePosition);
        }
        if (sideEffect == 2) {
            if (ledger == null || ledger.state() != AttemptLedgerState.PUBLISHING) {
                return persistSystemResult(
                        mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
            }
            if (!matchesRetainedOutcomeCharge(ledger, outcome.transfer())) {
                return persistSystemResult(
                        mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.STALE_SYSTEM_MUTATION);
            }
            final SystemMutationResult result =
                    SystemMutationResult.from(mutation, ApplyStatus.APPLIED, code, sourcePosition.canonicalBytes());
            return applyNotPublishedPublishOutcome(
                    ledger, outcome, sourcePosition, result, AttemptLedgerState.PUBLISHING, MessageStatus.PUBLISHING);
        }
        if (sideEffect == 1) {
            if (!matchesRetainedOutcomeCharge(ledger, outcome.transfer())) {
                return persistSystemResult(
                        mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.STALE_SYSTEM_MUTATION);
            }
            final SystemMutationResult result =
                    SystemMutationResult.from(mutation, ApplyStatus.APPLIED, code, sourcePosition.canonicalBytes());
            try {
                applyPublishedPublishOutcome(
                        ledger,
                        sourcePosition,
                        result,
                        MessageStatus.PUBLISHING,
                        publishedStatusForOutcome(ledger, outcome));
                return result;
            } catch (IllegalStateException | IllegalArgumentException exception) {
                return persistSystemResultByResult(result, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
            }
        }
        if (sideEffect == 3) {
            if (disposition == 0 || code == StableCode.OK || evidence.length != 0) {
                return persistSystemResult(
                        mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.STALE_SYSTEM_MUTATION);
            }
            final SystemMutationResult result =
                    SystemMutationResult.from(mutation, ApplyStatus.APPLIED, code, sourcePosition.canonicalBytes());
            applyUnknownPublishOutcome(
                    attemptId,
                    ledger.ownerEpoch(),
                    mutation.canonicalBody(),
                    evidence,
                    sourcePosition,
                    result,
                    outcome.retryDecision());
            return result;
        }
        return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
    }

    /**
     * The only initial Outcome that may be authored by a recovery Owner with
     * a different epoch. The live Oxia/source guard is external to this
     * local shard; keeping the tuple closed here prevents a cross-Owner
     * definitive result or a policy retry from bypassing that authority.
     */
    private static boolean isCrossOwnerRecoveryUnknown(final PublishOutcomeBody outcome) {
        return outcome.sideEffect() == 3
                && outcome.disposition() == 4
                && outcome.stableCode() == StableCode.RECOVERY_FIRST_SEND_UNCERTAIN
                && outcome.evidence().length == 0
                && outcome.retryDecision().kind() == 5
                && !outcome.retryDecision().hasNextRetryAt();
    }

    private static OwnerIdentity canonicalOwnerIdentity(final byte[] encoded) {
        try {
            return OwnerIdentity.decode(encoded);
        } catch (IllegalArgumentException exception) {
            return null;
        }
    }

    /**
     * New ledgers retain a canonical OwnerIdentity, while old embedded
     * fixtures used an opaque owner token. Preserve the latter compatibility
     * seam but never weaken the canonical identity comparison.
     */
    private static boolean matchesAdmittedOwner(final PublishAttemptLedger ledger, final AuthorIdentity author) {
        final OwnerIdentity admittedOwner = canonicalOwnerIdentity(ledger.ownerIdentity());
        if (admittedOwner != null) {
            return admittedOwner.ownerEpoch() == ledger.ownerEpoch()
                    && Arrays.equals(
                            ledger.ownerIdentity(), author.asOwnerIdentity().canonicalBytes());
        }
        return ledger.ownerEpoch() == author.generation();
    }

    /**
     * Existing publish obligations must never outlive their durable Lane
     * projection. Only canonical ledgers carry an authoritative Lane
     * incarnation; legacy embedded ledgers retain the historical opaque
     * incarnation compatibility seam while still requiring Lane presence.
     */
    private LaneRecord requirePublishAttemptLane(final PublishAttemptLedger ledger, final String operation) {
        final LaneRecord lane = readLane(ledger.laneId());
        if (lane == null) {
            throw new IllegalStateException(operation + " references a missing Lane");
        }
        if (ledger.hasRetryWindow() && !Arrays.equals(lane.laneIncarnation(), ledger.laneIncarnation())) {
            throw new IllegalStateException(operation + " references a mismatched Lane incarnation");
        }
        return lane;
    }

    /**
     * Recomputes the local Retry Policy projection before a source-ordered
     * outcome changes durable timeline state. Compatibility shards without a
     * source-pinned catalog retain their legacy structural-only behavior; a
     * catalogued binding must match the immutable ref, deadline and
     * deterministic full-jitter timestamp exactly.
     */
    private void validateRetryDecisionBinding(
            final PublishOutcomeBody outcome,
            final PublishAttemptLedger ledger,
            final MessageRecord current,
            final SourcePosition sourcePosition) {
        final PublishOutcomeBody.RetryDecision decision = outcome.retryDecision();
        if (!decision.hasFullShape()) {
            return;
        }
        final Long admittedFirstAttemptAt;
        if (ledger.hasRetryWindow()) {
            admittedFirstAttemptAt = ledger.firstAttemptAtEpochMs();
        } else {
            admittedFirstAttemptAt = admittedFirstAttemptAt(ledger);
        }
        if (admittedFirstAttemptAt != null && decision.firstAttemptAt() != admittedFirstAttemptAt) {
            throw new IllegalArgumentException("RetryDecision first attempt does not match Publish Admission");
        }
        if (ledger.hasRetryWindow() && decision.retryDeadline() != ledger.retryDeadlineEpochMs()) {
            throw new IllegalArgumentException("RetryDecision deadline does not match the attempt ledger");
        }
        if (retryPolicyCatalog == null) {
            // A compatibility shard without a policy catalog cannot recompute
            // exponential cap/jitter, but a Current ledger still supplies the
            // immutable first-attempt/deadline fact and must not be bypassed.
            if (ledger.hasRetryWindow()
                    && decision.hasNextRetryAt()
                    && decision.nextRetryAt() > ledger.retryDeadlineEpochMs()) {
                throw new IllegalArgumentException("RetryDecision next retry exceeds the attempt ledger deadline");
            }
            return;
        }
        final RetryPolicySemantic policy = retryPolicyFor(ledger.delayMessageId(), current, sourcePosition);
        if (policy == null
                || !decision.policy().matches(policy)
                || decision.retryDomain() != RetryJitter.MESSAGE_PUBLISH
                || decision.completedAttemptNo() != UnsignedInt32.toLong(ledger.attemptNo())
                || decision.firstAttemptAt() >= current.expireAtEpochMs()) {
            throw new IllegalArgumentException("RetryDecision does not match the pinned Retry Policy");
        }
        final long expectedDeadline =
                retryDeadlineForAdmission(decision.firstAttemptAt(), current.expireAtEpochMs(), policy);
        if (ledger.hasRetryWindow() && ledger.retryDeadlineEpochMs() != expectedDeadline) {
            throw new IllegalArgumentException("attempt ledger retry deadline does not match the pinned policy");
        }
        if (decision.retryDeadline() != expectedDeadline) {
            throw new IllegalArgumentException("RetryDecision deadline does not match the pinned Retry Policy");
        }
        if (decision.hasNextRetryAt()) {
            final long backoffCap = policy.retryBackoffCap(decision.completedAttemptNo());
            final long jitter = RetryJitter.delayMs(
                    RetryJitter.MESSAGE_PUBLISH,
                    ledger.delayMessageId(),
                    UnsignedInt32.toLong(ledger.generation()),
                    decision.completedAttemptNo(),
                    backoffCap);
            final long expectedNext = Math.addExact(outcome.observedAt().latestEpochMs(), jitter);
            if (decision.nextRetryAt() != expectedNext || expectedNext > decision.retryDeadline()) {
                throw new IllegalArgumentException("RetryDecision next retry does not match deterministic jitter");
            }
        }
    }

    private static long retryDeadlineForAdmission(
            final long firstAttemptAt, final long expireAt, final RetryPolicySemantic policy) {
        if (firstAttemptAt < 0 || expireAt < firstAttemptAt) {
            throw new IllegalArgumentException("invalid Admission retry window");
        }
        if (policy == null) {
            // Legacy/non-catalogued schedules have no immutable policy budget;
            // the message expiry remains the only safe local upper bound.
            return expireAt;
        }
        try {
            return Math.min(expireAt, Math.addExact(firstAttemptAt, policy.maxRetryDurationMs()));
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("Admission retry deadline arithmetic overflow", overflow);
        }
    }

    /** Validates Current ledger facts before an attempt can become durable. */
    private void validatePersistedRetryWindow(
            final PublishAttemptLedger admission,
            final MessageRecord current,
            final LaneRecord lane,
            final SourcePosition sourcePosition) {
        if (!admission.hasRetryWindow()) {
            return;
        }
        final PublishAdmissionBody body;
        try {
            body = PublishAdmissionBody.decode(admission.admissionBytes());
        } catch (IllegalArgumentException malformed) {
            throw new IllegalArgumentException("typed retry window requires a canonical Publish Admission", malformed);
        }
        if (!Arrays.equals(body.publishAttemptId(), admission.publishAttemptId())
                || body.generation() != admission.generation()
                || !Arrays.equals(body.messageId(), admission.delayMessageId().bytes())
                || !Arrays.equals(body.claimId(), admission.claimId())
                || !Arrays.equals(body.laneId(), admission.laneId().bytes())
                || !Arrays.equals(body.laneIncarnation(), admission.laneIncarnation())
                || !Arrays.equals(body.ownerIdentity(), admission.ownerIdentity())
                || !Arrays.equals(body.storeIncarnation(), admission.storeIncarnation())
                || !Arrays.equals(body.preparedPublishHash(), admission.preparedPublishHash())
                || body.descriptor().attemptNo() != admission.attemptNo()) {
            throw new IllegalArgumentException("typed retry window is bound to another Admission");
        }
        if (!Arrays.equals(body.laneId(), lane.laneId().bytes())
                || !Arrays.equals(body.laneIncarnation(), lane.laneIncarnation())) {
            throw new IllegalArgumentException("typed retry window Lane identity is stale");
        }
        final OwnerIdentity owner = OwnerIdentity.decode(body.ownerIdentity());
        if (owner.ownerEpoch() != admission.ownerEpoch()) {
            throw new IllegalArgumentException("typed retry window owner generation is stale");
        }
        if (body.descriptor().deliverAtEpochMs() != current.deliverAtEpochMs()
                || body.descriptor().expireAtEpochMs() != current.expireAtEpochMs()) {
            throw new IllegalArgumentException("typed retry window message timing is stale");
        }
        final RetryPolicySemantic policy = retryPolicyFor(admission.delayMessageId(), current, sourcePosition);
        final long expectedFirst = body.decisionTime().latestEpochMs();
        final long expectedDeadline = retryDeadlineForAdmission(expectedFirst, current.expireAtEpochMs(), policy);
        if (admission.firstAttemptAtEpochMs() != expectedFirst
                || admission.retryDeadlineEpochMs() != expectedDeadline) {
            throw new IllegalArgumentException("persisted retry window does not match Admission/policy");
        }
    }

    /**
     * Canonical ledgers retain the complete Admission body, whose trusted
     * decision interval is the durable first-attempt fact. Synthetic legacy
     * ledgers may carry opaque bytes; keep that bounded compatibility seam but
     * never downgrade a body that claims the canonical System Mutation shape.
     */
    private static Long admittedFirstAttemptAt(final PublishAttemptLedger ledger) {
        try {
            return PublishAdmissionBody.decode(ledger.admissionBytes())
                    .decisionTime()
                    .latestEpochMs();
        } catch (IllegalArgumentException malformedAdmission) {
            final byte[] encoded = ledger.admissionBytes();
            if (encoded.length > 0 && (encoded[0] & 0xff) == 0x0a) {
                throw new IllegalArgumentException(
                        "malformed canonical PUBLISH_ADMISSION in attempt ledger", malformedAdmission);
            }
            return null;
        }
    }

    private SystemMutationResult applyEvidenceResolutionMutation(
            final SystemMutation mutation, final SourcePosition sourcePosition) {
        final PublishOutcomeBody resolution = PublishOutcomeBody.decodeEvidenceResolution(mutation.canonicalBody());
        if (!Bytes.constantTimeEquals(
                mutation.logicalOperationIdentity(), resolution.evidenceResolutionLogicalOperationIdentity())) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final PublishAttemptLedger ledger = findOpenPublishAttempt(resolution.publishAttemptId());
        if (ledger == null || ledger.state() != AttemptLedgerState.UNCERTAIN) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
        requirePublishAttemptLane(ledger, "evidence resolution");
        final MessageRecord resolutionCurrent = getMessage(ledger.delayMessageId());
        if (resolutionCurrent != null && resolutionCurrent.generation() == ledger.generation()) {
            validateRetryDecisionBinding(resolution, ledger, resolutionCurrent, sourcePosition);
        }
        if (!matchesRetainedOutcomeCharge(ledger, resolution.transfer())) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.STALE_SYSTEM_MUTATION);
        }
        final SystemMutationResult result = SystemMutationResult.from(
                mutation, ApplyStatus.APPLIED, resolution.stableCode(), sourcePosition.canonicalBytes());
        if (resolution.sideEffect() == 1) {
            try {
                applyPublishedPublishOutcome(
                        ledger,
                        sourcePosition,
                        result,
                        MessageStatus.UNCERTAIN,
                        publishedStatusForOutcome(ledger, resolution));
                return result;
            } catch (IllegalStateException | IllegalArgumentException exception) {
                return persistSystemResultByResult(result, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
            }
        }
        return applyNotPublishedPublishOutcome(
                ledger, resolution, sourcePosition, result, AttemptLedgerState.UNCERTAIN, MessageStatus.UNCERTAIN);
    }

    private static StableCode laneAdmissionClosedCode(final AdmissionGate gate) {
        return switch (gate) {
            case CLOSED -> StableCode.LANE_CLOSED;
            case RETIRED -> StableCode.LANE_TERMINALLY_CLOSED;
            default -> throw new IllegalArgumentException("Lane is not closed: " + gate);
        };
    }

    /**
     * Applies the source-ordered Resolve subset. Verified-published evidence
     * can settle the exact UNCERTAIN obligation locally; absent evidence and
     * possible-delivery terminalization retain their explicit source-ordered
     * branches until the remaining result/charge projection is available.
     */
    private SystemMutationResult applyResolveUncertainMutation(
            final SystemMutation mutation, final SourcePosition sourcePosition) {
        final ResolveUncertainBody body = ResolveUncertainBody.decode(mutation.canonicalBody());
        if (!Arrays.equals(
                mutation.logicalOperationIdentity(),
                body.controlRef().logicalOperationIdentity(SystemMutationType.RESOLVE_UNCERTAIN))) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
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
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
        final MessageRecord current = getMessage(body.messageId());
        if (current == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }
        if (current.generation() != body.generation()) {
            return persistSystemResult(
                    mutation,
                    sourcePosition,
                    ApplyStatus.APPLIED,
                    compareGeneration(current.generation(), body.generation()) > 0
                            ? StableCode.GENERATION_SUPERSEDED
                            : StableCode.STALE_SYSTEM_MUTATION);
        }
        if (!current.laneId().equals(body.laneId())
                || current.status() != MessageStatus.UNCERTAIN
                || current.runtimeIndex().currentWorkKind() != CurrentSendWorkKind.NONE
                || current.orderingMode() != com.nereusstream.delay.protocol.OrderingMode.BEST_EFFORT) {
            return persistSystemResult(
                    mutation,
                    sourcePosition,
                    ApplyStatus.APPLIED,
                    current.orderingMode() != com.nereusstream.delay.protocol.OrderingMode.BEST_EFFORT
                            ? StableCode.ORDERING_DOMAIN_BROKEN
                            : StableCode.TOO_LATE);
        }
        final LaneRecord lane = readLane(body.laneId());
        if (lane == null) {
            throw new IllegalStateException("Resolve retry references a missing Lane");
        }
        if (!Arrays.equals(lane.laneIncarnation(), body.laneIncarnation())) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
        if (lane.admissionGate() == AdmissionGate.ORDERING_BROKEN) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.ORDERING_DOMAIN_BROKEN);
        }
        if (lane.admissionGate() == AdmissionGate.CLOSED || lane.admissionGate() == AdmissionGate.RETIRED) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.APPLIED, laneAdmissionClosedCode(lane.admissionGate()));
        }
        final AttemptObligationRef target = current.runtimeIndex().attemptObligations().stream()
                .filter(ref -> Arrays.equals(ref.publishAttemptId(), body.publishAttemptId())
                        && ref.generation() == body.generation()
                        && ref.ledgerState() == AttemptLedgerState.UNCERTAIN)
                .findFirst()
                .orElse(null);
        if (target == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }
        final PublishAttemptLedger ledger = readLedgerForObligation(target);
        if (ledger.state() != AttemptLedgerState.UNCERTAIN
                || !ledger.delayMessageId().equals(body.messageId())
                || !ledger.laneId().equals(body.laneId())
                || ledger.generation() != body.generation()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
        final int candidateAttemptNo;
        try {
            candidateAttemptNo = Math.addExact(current.runtimeIndex().admissionsUsed(), 1);
        } catch (ArithmeticException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }
        final RetryPolicySemantic pinnedPolicy = retryPolicyFor(body.messageId(), current, sourcePosition);
        final int maxPublishAdmissions =
                pinnedPolicy == null ? config.maxPublishAdmissions() : pinnedPolicy.maxPublishAdmissions();
        if (current.runtimeIndex().admissionsUsed() >= maxPublishAdmissions) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }
        final long retryAt = Math.max(current.deliverAtEpochMs(), sourcePosition.brokerPersistenceTimeEpochMs());
        if (retryAt >= current.expireAtEpochMs()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }
        MessageRecord scheduled = MessageRecord.current(
                MessageStatus.SCHEDULED,
                current.generation(),
                Math.addExact(current.stateVersion(), 1),
                current.deliverAtEpochMs(),
                current.expireAtEpochMs(),
                current.laneId(),
                current.orderingMode(),
                current.payload(),
                current.scheduleSourcePosition(),
                current.payloadReference(),
                retryAt);
        scheduled = scheduled.withRuntimeIndex(timelineRuntimeIndex(
                body.messageId(),
                scheduled,
                TimelineWorkKind.UNCERTAIN_RETRY,
                candidateAttemptNo,
                scheduled.stateVersion(),
                UncertainRetryAuthority.CONTROL_OVERRIDE,
                body.controlRef().canonicalBytes(),
                sourcePosition.canonicalBytes(),
                current.runtimeIndex(),
                current.runtimeIndex().attemptObligations()));
        final MessageRecord scheduledForWrite = scheduled;
        final Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections =
                readyProjections(sourcePosition, body.messageId(), current, scheduled, null, laneQuotaUsage);
        final SystemMutationResult result = SystemMutationResult.from(
                mutation, ApplyStatus.APPLIED, StableCode.OK, sourcePosition.canonicalBytes());
        store.write(batch -> {
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(body.messageId()), scheduledForWrite.encode());
            batch.putValue(
                    ColumnFamily.TIMELINE,
                    1,
                    timelineKey(body.messageId(), scheduledForWrite),
                    encodeTimelineValue(body.messageId(), scheduledForWrite));
            batch.putValue(
                    ColumnFamily.TIMELINE,
                    1,
                    expiryKey(body.messageId(), scheduledForWrite),
                    encodeTimelineValue(body.messageId(), scheduledForWrite));
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        return result;
    }

    /** Settles a verified-published Resolve attachment against one exact obligation. */
    private SystemMutationResult applyPublishedEvidenceAttachment(
            final ResolveUncertainBody body, final SystemMutation mutation, final SourcePosition sourcePosition) {
        final MessageRecord current = getMessage(body.messageId());
        if (current == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }
        if (compareGeneration(current.generation(), body.generation()) < 0) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
        final PublishAttemptLedger ledger = findOpenPublishAttempt(body.publishAttemptId());
        if (ledger == null
                || ledger.state() != AttemptLedgerState.UNCERTAIN
                || !ledger.delayMessageId().equals(body.messageId())
                || !ledger.laneId().equals(body.laneId())
                || !Arrays.equals(ledger.laneIncarnation(), body.laneIncarnation())
                || ledger.generation() != body.generation()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
        requirePublishAttemptLane(ledger, "published evidence");
        if (current.generation() == body.generation() && !current.laneId().equals(body.laneId())) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }
        if (compareGeneration(current.generation(), body.generation()) > 0) {
            final TerminalGenerationRecord summary = getTerminalGeneration(body.messageId(), body.generation());
            if (summary == null || !summary.openObligations().contains(ledger.obligationRef())) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
            }
        }
        final SystemMutationResult result = SystemMutationResult.from(
                mutation, ApplyStatus.APPLIED, StableCode.OK, sourcePosition.canonicalBytes());
        try {
            applyPublishedPublishOutcome(
                    ledger,
                    sourcePosition,
                    result,
                    MessageStatus.UNCERTAIN,
                    publishedStatusForEvidence(body.evidence(), ledger));
            return result;
        } catch (IllegalStateException | IllegalArgumentException exception) {
            return persistSystemResultByResult(result, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
        }
    }

    /**
     * Settles one exact UNCERTAIN obligation with authenticated definitive
     * non-publication evidence. The evidence branch never invents a retry
     * policy: once the named obligation is removed, the remaining runtime
     * index either stays uncertain, preserves another current PUBLISHING
     * attempt, or follows the ordinary all-absent definitive-retry
     * normalization.
     */
    private SystemMutationResult applyNotPublishedEvidenceAttachment(
            final ResolveUncertainBody body, final SystemMutation mutation, final SourcePosition sourcePosition) {
        final MessageRecord current = getMessage(body.messageId());
        if (current == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }
        if (compareGeneration(current.generation(), body.generation()) < 0) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
        final PublishAttemptLedger ledger = findOpenPublishAttempt(body.publishAttemptId());
        if (ledger == null
                || ledger.state() != AttemptLedgerState.UNCERTAIN
                || !ledger.delayMessageId().equals(body.messageId())
                || !ledger.laneId().equals(body.laneId())
                || !Arrays.equals(ledger.laneIncarnation(), body.laneIncarnation())
                || ledger.generation() != body.generation()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
        requirePublishAttemptLane(ledger, "not-published evidence");
        if (current.generation() == body.generation() && !current.laneId().equals(body.laneId())) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }
        if (compareGeneration(current.generation(), body.generation()) > 0) {
            final TerminalGenerationRecord summary = getTerminalGeneration(body.messageId(), body.generation());
            if (summary == null || !summary.openObligations().contains(ledger.obligationRef())) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
            }
            final SystemMutationResult result = SystemMutationResult.from(
                    mutation, ApplyStatus.APPLIED, StableCode.OK, sourcePosition.canonicalBytes());
            try {
                settleHistoricalTerminalObligation(ledger, sourcePosition, result, false);
                return result;
            } catch (IllegalStateException exception) {
                return persistSystemResultByResult(result, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
            }
        }

        final GenerationRuntimeIndex index = current.runtimeIndex();
        if (!containsObligation(index, ledger.obligationRef())) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
        if (isTerminalStatus(current.status())) {
            final SystemMutationResult result = SystemMutationResult.from(
                    mutation, ApplyStatus.APPLIED, StableCode.OK, sourcePosition.canonicalBytes());
            try {
                settleTerminalObligation(ledger, current, sourcePosition, result, false);
                return result;
            } catch (IllegalStateException exception) {
                return persistSystemResultByResult(result, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
            }
        }

        final LaneRecord lane = readLane(current.laneId());
        if (lane == null || !Arrays.equals(lane.laneIncarnation(), body.laneIncarnation())) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
        final List<AttemptObligationRef> remaining = withoutObligation(index, ledger.publishAttemptId());
        final boolean remainingUncertain =
                remaining.stream().anyMatch(ref -> ref.ledgerState() == AttemptLedgerState.UNCERTAIN);
        final SystemMutationResult result = SystemMutationResult.from(
                mutation, ApplyStatus.APPLIED, StableCode.OK, sourcePosition.canonicalBytes());
        try {
            if (remainingUncertain) {
                return settleNotPublishedUncertainObligation(ledger, current, remaining, sourcePosition, result);
            }
            if (index.currentWorkKind() == CurrentSendWorkKind.PUBLISHING) {
                return preservePublishingAfterNotPublishedEvidence(ledger, current, remaining, sourcePosition, result);
            }
            if (lane.admissionGate() == AdmissionGate.CLOSED || lane.admissionGate() == AdmissionGate.RETIRED) {
                return terminalizeNotPublishedEvidence(
                        ledger,
                        current,
                        remaining,
                        sourcePosition,
                        result,
                        MessageStatus.DEAD_LETTER,
                        StableCode.LANE_CLOSED_AFTER_ADMISSION_NOT_PUBLISHED);
            }

            final RetryPolicySemantic pinnedPolicy = retryPolicyFor(body.messageId(), current, sourcePosition);
            final int maxPublishAdmissions =
                    pinnedPolicy == null ? config.maxPublishAdmissions() : pinnedPolicy.maxPublishAdmissions();
            if (index.admissionsUsed() >= maxPublishAdmissions) {
                return terminalizeNotPublishedEvidence(
                        ledger,
                        current,
                        remaining,
                        sourcePosition,
                        result,
                        MessageStatus.DEAD_LETTER,
                        StableCode.DESTINATION_DEFINITIVE_PERMANENT);
            }
            final long retryAt = Math.max(
                    Math.max(current.deliverAtEpochMs(), current.retryEligibilityAtEpochMs()),
                    sourcePosition.brokerPersistenceTimeEpochMs());
            if (retryAt >= current.expireAtEpochMs()) {
                return terminalizeNotPublishedEvidence(
                        ledger,
                        current,
                        remaining,
                        sourcePosition,
                        result,
                        MessageStatus.EXPIRED,
                        StableCode.ALREADY_EXPIRED);
            }
            return normalizeDefinitiveRetryAfterNotPublishedEvidence(
                    ledger, current, remaining, retryAt, sourcePosition, result);
        } catch (IllegalStateException | ArithmeticException exception) {
            return persistSystemResultByResult(result, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
        }
    }

    /** Removes only the named obligation while preserving still-uncertain work. */
    private SystemMutationResult settleNotPublishedUncertainObligation(
            final PublishAttemptLedger ledger,
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
            nextRuntime = GenerationRuntimeIndex.timeline(
                    GenerationAggregateState.UNCERTAIN,
                    index.timeline(),
                    remaining,
                    index.admissionsUsed(),
                    index.uncertainRetryAdmissionsUsed(),
                    index.possibleDestinationDuplicate(),
                    Math.addExact(index.runtimeRevision(), 1));
            revokedClaim = null;
            nextStatus = current.status();
        } else if (index.currentWorkKind() == CurrentSendWorkKind.CLAIMED) {
            revokedClaim = findClaimForMessage(ledger.delayMessageId());
            if (revokedClaim == null
                    || !Arrays.equals(revokedClaim.claimId(), index.claimId())
                    || revokedClaim.runtimeRevision() != current.stateVersion()) {
                throw new IllegalStateException("uncertain Claim work is missing its exact record");
            }
            // The Claim precondition freezes the old obligation-set digest. Once
            // one old attempt is settled, retaining that Claim would make a later
            // Admission fail against an already-invalid digest. Revoke it
            // atomically and leave the generation UNCERTAIN/NONE; a subsequent
            // source-ordered Resolve retry can materialize a fresh timeline.
            nextRuntime = GenerationRuntimeIndex.none(
                    GenerationAggregateState.UNCERTAIN,
                    remaining,
                    index.admissionsUsed(),
                    index.uncertainRetryAdmissionsUsed(),
                    index.possibleDestinationDuplicate(),
                    Math.addExact(index.runtimeRevision(), 1));
            nextStatus = MessageStatus.UNCERTAIN;
        } else if (index.currentWorkKind() == CurrentSendWorkKind.PUBLISHING) {
            nextRuntime = GenerationRuntimeIndex.publishing(
                    index.publishAttemptId(),
                    remaining,
                    index.admissionsUsed(),
                    index.uncertainRetryAdmissionsUsed(),
                    index.possibleDestinationDuplicate(),
                    Math.addExact(index.runtimeRevision(), 1));
            revokedClaim = null;
            nextStatus = current.status();
        } else if (index.currentWorkKind() == CurrentSendWorkKind.NONE) {
            nextRuntime = GenerationRuntimeIndex.none(
                    GenerationAggregateState.UNCERTAIN,
                    remaining,
                    index.admissionsUsed(),
                    index.uncertainRetryAdmissionsUsed(),
                    index.possibleDestinationDuplicate(),
                    Math.addExact(index.runtimeRevision(), 1));
            revokedClaim = null;
            nextStatus = current.status();
        } else {
            throw new IllegalStateException("unsupported uncertain work kind");
        }
        final long nextStateVersion =
                nextStatus == current.status() ? current.stateVersion() : Math.addExact(current.stateVersion(), 1);
        final MessageRecord next = MessageRecord.current(
                        nextStatus,
                        current.generation(),
                        nextStateVersion,
                        current.deliverAtEpochMs(),
                        current.expireAtEpochMs(),
                        current.laneId(),
                        current.orderingMode(),
                        current.payload(),
                        current.scheduleSourcePosition(),
                        current.payloadReference(),
                        current.retryEligibilityAtEpochMs())
                .withRuntimeIndex(nextRuntime);
        final OutcomeReserveUsage nextOutcomeReserve = releasedOutcomeReserve(ledger);
        final CapacityVector nextOutcomeReserveVector = releasedOutcomeReserveVector(ledger);
        LaneQuotaUsageProjection nextLaneQuota = mutateInflightQuotaUsage(
                laneQuotaUsage,
                ledger.laneId(),
                ledger.laneIncarnation(),
                attemptCharge(ledger),
                false,
                Math.max(1, quota.usageRevision()));
        if (revokedClaim != null) {
            nextLaneQuota = mutateInflightQuotaUsage(
                    nextLaneQuota,
                    revokedClaim.laneId(),
                    revokedClaim.laneIncarnation(),
                    claimCharge(revokedClaim),
                    false,
                    Math.max(1, quota.usageRevision()));
        }
        final LaneQuotaUsageProjection projectedLaneQuota = nextLaneQuota;
        store.write(batch -> {
            batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
            if (revokedClaim != null) {
                batch.delete(ColumnFamily.INFLIGHT, revokedClaim.encodedKey());
            }
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(ledger.delayMessageId()), next.encode());
            persistQuota(batch, quota, projectedLaneQuota);
            persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector, projectedLaneQuota);
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        laneQuotaUsage = projectedLaneQuota;
        outcomeReserve = nextOutcomeReserve;
        outcomeReserveVector = nextOutcomeReserveVector;
        return result;
    }

    /** Keeps a different admitted send as the sole current work item. */
    private SystemMutationResult preservePublishingAfterNotPublishedEvidence(
            final PublishAttemptLedger ledger,
            final MessageRecord current,
            final List<AttemptObligationRef> remaining,
            final SourcePosition sourcePosition,
            final SystemMutationResult result) {
        final GenerationRuntimeIndex index = current.runtimeIndex();
        if (index.publishAttemptId().length == 0
                || Arrays.equals(index.publishAttemptId(), ledger.publishAttemptId())) {
            throw new IllegalStateException("not-published evidence cannot remove current publishing work");
        }
        final GenerationRuntimeIndex nextRuntime = GenerationRuntimeIndex.publishing(
                index.publishAttemptId(),
                remaining,
                index.admissionsUsed(),
                index.uncertainRetryAdmissionsUsed(),
                index.possibleDestinationDuplicate(),
                Math.addExact(index.runtimeRevision(), 1));
        final MessageRecord next = MessageRecord.current(
                        current.status(),
                        current.generation(),
                        current.stateVersion(),
                        current.deliverAtEpochMs(),
                        current.expireAtEpochMs(),
                        current.laneId(),
                        current.orderingMode(),
                        current.payload(),
                        current.scheduleSourcePosition(),
                        current.payloadReference(),
                        current.retryEligibilityAtEpochMs())
                .withRuntimeIndex(nextRuntime);
        final OutcomeReserveUsage nextOutcomeReserve = releasedOutcomeReserve(ledger);
        final CapacityVector nextOutcomeReserveVector = releasedOutcomeReserveVector(ledger);
        final LaneQuotaUsageProjection nextLaneQuota = mutateInflightQuotaUsage(
                laneQuotaUsage,
                ledger.laneId(),
                ledger.laneIncarnation(),
                attemptCharge(ledger),
                false,
                Math.max(1, quota.usageRevision()));
        store.write(batch -> {
            batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(ledger.delayMessageId()), next.encode());
            persistQuota(batch, quota, nextLaneQuota);
            persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector, nextLaneQuota);
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        laneQuotaUsage = nextLaneQuota;
        outcomeReserve = nextOutcomeReserve;
        outcomeReserveVector = nextOutcomeReserveVector;
        return result;
    }

    /** Converts reversible timeline/Claim/NONE work to a definitive retry. */
    private SystemMutationResult normalizeDefinitiveRetryAfterNotPublishedEvidence(
            final PublishAttemptLedger ledger,
            final MessageRecord current,
            final List<AttemptObligationRef> remaining,
            final long retryAt,
            final SourcePosition sourcePosition,
            final SystemMutationResult result) {
        final GenerationRuntimeIndex index = current.runtimeIndex();
        final byte[] priorTimelineKey;
        final byte[] claimKey;
        if (index.currentWorkKind() == CurrentSendWorkKind.TIMELINE) {
            if (index.timeline() == null
                    || !Arrays.equals(
                            index.timeline().encodedTimelineKey(), timelineKey(ledger.delayMessageId(), current))) {
                throw new IllegalStateException("definitive retry timeline identity is stale");
            }
            priorTimelineKey = index.timeline().encodedTimelineKey();
            claimKey = null;
        } else if (index.currentWorkKind() == CurrentSendWorkKind.CLAIMED) {
            final ClaimRecord claim = findClaimForMessage(ledger.delayMessageId());
            if (claim == null
                    || !Arrays.equals(claim.claimId(), index.claimId())
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
                ? index.timeline().candidateAttemptNo()
                : Math.addExact(index.admissionsUsed(), 1);
        final MessageRecord scheduled = MessageRecord.current(
                MessageStatus.SCHEDULED,
                current.generation(),
                Math.addExact(current.stateVersion(), 1),
                current.deliverAtEpochMs(),
                current.expireAtEpochMs(),
                current.laneId(),
                current.orderingMode(),
                current.payload(),
                current.scheduleSourcePosition(),
                current.payloadReference(),
                retryAt);
        final MessageRecord scheduledForWrite = scheduled.withRuntimeIndex(timelineRuntimeIndex(
                ledger.delayMessageId(),
                scheduled,
                TimelineWorkKind.DEFINITIVE_RETRY,
                candidateAttemptNo,
                scheduled.stateVersion(),
                UncertainRetryAuthority.NONE,
                null,
                null,
                current.runtimeIndex(),
                remaining));
        final OutcomeReserveUsage nextOutcomeReserve = releasedOutcomeReserve(ledger);
        final CapacityVector nextOutcomeReserveVector = releasedOutcomeReserveVector(ledger);
        LaneQuotaUsageProjection nextLaneQuota = mutateInflightQuotaUsage(
                laneQuotaUsage,
                ledger.laneId(),
                ledger.laneIncarnation(),
                attemptCharge(ledger),
                false,
                Math.max(1, quota.usageRevision()));
        if (claimKey != null) {
            final ClaimRecord claim = findClaimForMessage(ledger.delayMessageId());
            if (claim != null) {
                nextLaneQuota = mutateInflightQuotaUsage(
                        nextLaneQuota,
                        claim.laneId(),
                        claim.laneIncarnation(),
                        claimCharge(claim),
                        false,
                        Math.max(1, quota.usageRevision()));
            }
        }
        final LaneQuotaUsageProjection projectedLaneQuota = nextLaneQuota;
        final Map<DestinationLaneId, LaneProjection> projections = readyProjections(
                sourcePosition, ledger.delayMessageId(), current, scheduledForWrite, null, projectedLaneQuota);
        store.write(batch -> {
            if (priorTimelineKey != null) {
                batch.delete(ColumnFamily.TIMELINE, priorTimelineKey);
                batch.delete(ColumnFamily.TIMELINE, expiryKey(ledger.delayMessageId(), current));
            }
            if (claimKey != null) {
                batch.delete(ColumnFamily.INFLIGHT, claimKey);
            }
            batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(ledger.delayMessageId()), scheduledForWrite.encode());
            batch.putValue(
                    ColumnFamily.TIMELINE,
                    1,
                    timelineKey(ledger.delayMessageId(), scheduledForWrite),
                    encodeTimelineValue(ledger.delayMessageId(), scheduledForWrite));
            batch.putValue(
                    ColumnFamily.TIMELINE,
                    1,
                    expiryKey(ledger.delayMessageId(), scheduledForWrite),
                    encodeTimelineValue(ledger.delayMessageId(), scheduledForWrite));
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            persistQuota(batch, quota, projectedLaneQuota);
            persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector, projectedLaneQuota);
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        laneQuotaUsage = projectedLaneQuota;
        outcomeReserve = nextOutcomeReserve;
        outcomeReserveVector = nextOutcomeReserveVector;
        return result;
    }

    /** Terminalizes after definitive absence when no further admission is safe. */
    private SystemMutationResult terminalizeNotPublishedEvidence(
            final PublishAttemptLedger ledger,
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
        final MessageRecord terminalMessage = MessageRecord.current(
                        terminalStatus,
                        current.generation(),
                        Math.addExact(current.stateVersion(), 1),
                        current.deliverAtEpochMs(),
                        current.expireAtEpochMs(),
                        current.laneId(),
                        current.orderingMode(),
                        current.payload(),
                        current.scheduleSourcePosition(),
                        current.payloadReference(),
                        current.retryEligibilityAtEpochMs())
                .withRuntimeIndex(nextRuntime);
        final TerminalGenerationRecord terminal = new TerminalGenerationRecord(
                ledger.delayMessageId(),
                ledger.generation(),
                terminalStatus,
                terminalCode,
                terminalMessage.stateVersion(),
                sourcePosition.canonicalBytes(),
                terminalMessage.runtimeIndex().possibleDestinationDuplicate(),
                remaining);
        final DlqExportRecord dlqExport = terminalStatus == MessageStatus.DEAD_LETTER
                ? DlqExportRecord.notConfigured(
                        ledger.delayMessageId(),
                        ledger.generation(),
                        terminalMessage.stateVersion(),
                        sourcePosition.canonicalBytes())
                : null;
        final ShardQuota nextQuota = quota.removeSchedule(current.payloadLength());
        LaneQuotaUsageProjection nextLaneQuota = removeScheduleQuotaUsage(current, nextQuota);
        nextLaneQuota = mutateInflightQuotaUsage(
                nextLaneQuota,
                ledger.laneId(),
                ledger.laneIncarnation(),
                attemptCharge(ledger),
                false,
                Math.max(1, nextQuota.usageRevision()));
        final LaneQuotaUsageProjection projectedLaneQuota = nextLaneQuota;
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
                if (claim == null
                        || !Arrays.equals(
                                claim.claimId(), current.runtimeIndex().claimId())
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
        final OutcomeReserveUsage nextOutcomeReserve = releasedOutcomeReserve(ledger);
        final CapacityVector nextOutcomeReserveVector = releasedOutcomeReserveVector(ledger);
        final Map<DestinationLaneId, LaneProjection> projections = readyProjections(
                sourcePosition, ledger.delayMessageId(), current, terminalMessage, null, projectedLaneQuota);
        final SystemMutationResult result = terminalStatus == MessageStatus.DEAD_LETTER
                ? new SystemMutationResult(
                        originalResult.mutationId(),
                        originalResult.mutationHash(),
                        originalResult.mutationType(),
                        originalResult.retryUntilEpochMs(),
                        originalResult.authorIdentity(),
                        originalResult.applyStatus(),
                        terminalCode,
                        sourcePosition.canonicalBytes())
                : originalResult;
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
            batch.putValue(
                    ColumnFamily.TERMINAL,
                    1,
                    KeyCodec.terminalGeneration(ledger.delayMessageId(), ledger.generation()),
                    terminal.encode());
            if (dlqExport != null) {
                batch.putValue(
                        ColumnFamily.TERMINAL,
                        DlqExportRecord.VALUE_TYPE,
                        KeyCodec.terminalDlqExport(dlqExport.dlqExportId()),
                        dlqExport.encode());
            }
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            persistQuota(batch, nextQuota, projectedLaneQuota);
            persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector, projectedLaneQuota);
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        quota = nextQuota;
        laneQuotaUsage = projectedLaneQuota;
        outcomeReserve = nextOutcomeReserve;
        outcomeReserveVector = nextOutcomeReserveVector;
        return result;
    }

    /** Terminalizes an unresolved generation while retaining its exact obligation ledger. */
    private SystemMutationResult applyPossibleDeliveryTerminalization(
            final ResolveUncertainBody body, final SystemMutation mutation, final SourcePosition sourcePosition) {
        final MessageRecord current = getMessage(body.messageId());
        if (current == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }
        if (current.generation() != body.generation()) {
            return persistSystemResult(
                    mutation,
                    sourcePosition,
                    ApplyStatus.APPLIED,
                    compareGeneration(current.generation(), body.generation()) > 0
                            ? StableCode.GENERATION_SUPERSEDED
                            : StableCode.STALE_SYSTEM_MUTATION);
        }
        if (!current.laneId().equals(body.laneId())
                || current.status() != MessageStatus.UNCERTAIN
                || current.runtimeIndex().currentWorkKind() != CurrentSendWorkKind.NONE) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }
        final LaneRecord lane = readLane(body.laneId());
        if (lane == null) {
            throw new IllegalStateException("Resolve terminalization references a missing Lane");
        }
        if (!Arrays.equals(lane.laneIncarnation(), body.laneIncarnation())) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
        final AttemptObligationRef target = current.runtimeIndex().attemptObligations().stream()
                .filter(ref -> Arrays.equals(ref.publishAttemptId(), body.publishAttemptId())
                        && ref.generation() == body.generation()
                        && ref.ledgerState() == AttemptLedgerState.UNCERTAIN)
                .findFirst()
                .orElse(null);
        if (target == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }
        final PublishAttemptLedger ledger = readLedgerForObligation(target);
        if (ledger.state() != AttemptLedgerState.UNCERTAIN
                || !ledger.delayMessageId().equals(body.messageId())
                || !ledger.laneId().equals(body.laneId())) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
        final MessageRecord terminalMessage = MessageRecord.current(
                        MessageStatus.DEAD_LETTER,
                        current.generation(),
                        Math.addExact(current.stateVersion(), 1),
                        current.deliverAtEpochMs(),
                        current.expireAtEpochMs(),
                        current.laneId(),
                        current.orderingMode(),
                        current.payload(),
                        current.scheduleSourcePosition(),
                        current.payloadReference(),
                        current.retryEligibilityAtEpochMs())
                .withRuntimeIndex(GenerationRuntimeIndex.none(
                        GenerationAggregateState.DEAD_LETTER,
                        current.runtimeIndex().attemptObligations(),
                        current.runtimeIndex().admissionsUsed(),
                        current.runtimeIndex().uncertainRetryAdmissionsUsed(),
                        true,
                        Math.addExact(current.runtimeIndex().runtimeRevision(), 1)));
        final TerminalGenerationRecord terminal = new TerminalGenerationRecord(
                body.messageId(),
                body.generation(),
                MessageStatus.DEAD_LETTER,
                StableCode.DESTINATION_OUTCOME_UNKNOWN,
                terminalMessage.stateVersion(),
                sourcePosition.canonicalBytes(),
                true,
                terminalMessage.runtimeIndex().attemptObligations());
        final DlqExportRecord dlqExport = DlqExportRecord.notConfigured(
                body.messageId(), body.generation(), terminalMessage.stateVersion(), sourcePosition.canonicalBytes());
        final ShardQuota nextQuota;
        try {
            nextQuota = quota.removeSchedule(current.payloadLength());
        } catch (IllegalStateException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.INTEGRITY_ERROR);
        }
        final LaneQuotaUsageProjection nextLaneQuota = removeScheduleQuotaUsage(current, nextQuota);
        final Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections =
                readyProjections(sourcePosition, body.messageId(), current, terminalMessage, null, nextLaneQuota);
        final SystemMutationResult result = SystemMutationResult.from(
                mutation, ApplyStatus.APPLIED, StableCode.DESTINATION_OUTCOME_UNKNOWN, sourcePosition.canonicalBytes());
        store.write(batch -> {
            batch.delete(ColumnFamily.TIMELINE, expiryKey(body.messageId(), current));
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(body.messageId()), terminalMessage.encode());
            batch.putValue(
                    ColumnFamily.TERMINAL,
                    1,
                    KeyCodec.terminalGeneration(body.messageId(), body.generation()),
                    terminal.encode());
            batch.putValue(
                    ColumnFamily.TERMINAL,
                    DlqExportRecord.VALUE_TYPE,
                    KeyCodec.terminalDlqExport(dlqExport.dlqExportId()),
                    dlqExport.encode());
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            persistQuota(batch, nextQuota, nextLaneQuota);
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        quota = nextQuota;
        laneQuotaUsage = nextLaneQuota;
        return result;
    }

    /** Applies the bounded source-ordered Dead Letter replay generation transition. */
    private SystemMutationResult applyReplayDeadLetterMutation(
            final SystemMutation mutation, final SourcePosition sourcePosition) {
        final ReplayDeadLetterBody body = ReplayDeadLetterBody.decode(mutation.canonicalBody());
        if (!Arrays.equals(
                mutation.logicalOperationIdentity(),
                body.controlRef().logicalOperationIdentity(SystemMutationType.REPLAY_DEAD_LETTER))) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final MessageRecord current = getMessage(body.messageId());
        if (current == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.NOT_FOUND);
        }
        if (current.generation() != body.expectedGeneration()) {
            return persistSystemResult(
                    mutation,
                    sourcePosition,
                    ApplyStatus.APPLIED,
                    compareGeneration(current.generation(), body.expectedGeneration()) > 0
                            ? StableCode.GENERATION_SUPERSEDED
                            : StableCode.STALE_SYSTEM_MUTATION);
        }
        if (current.stateVersion() != body.expectedStateVersion()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.VERSION_CONFLICT);
        }
        if (current.status() != MessageStatus.DEAD_LETTER) {
            return persistSystemResult(
                    mutation,
                    sourcePosition,
                    ApplyStatus.APPLIED,
                    current.status() == MessageStatus.PUBLISHED || current.status() == MessageStatus.HANDED_OFF
                            ? StableCode.ALREADY_PUBLISHED
                            : StableCode.TOO_LATE);
        }
        final TerminalGenerationRecord summary = getTerminalGeneration(body.messageId(), body.expectedGeneration());
        if (summary == null || summary.status() != MessageStatus.DEAD_LETTER) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.INTEGRITY_ERROR);
        }
        final boolean needsDuplicateAcknowledgement = summary.possibleDestinationDuplicate()
                || !summary.openObligations().isEmpty();
        if (needsDuplicateAcknowledgement != body.allowPossibleDuplicate()
                || needsDuplicateAcknowledgement && body.acknowledgementHash().length == 0) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }
        final LaneRecord lane = readLane(current.laneId());
        if (lane == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.INTEGRITY_ERROR);
        }
        if (lane.admissionGate() == AdmissionGate.CLOSED || lane.admissionGate() == AdmissionGate.RETIRED) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.APPLIED, laneAdmissionClosedCode(lane.admissionGate()));
        }
        if (lane.admissionGate() == AdmissionGate.ORDERING_BROKEN) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.ORDERING_DOMAIN_BROKEN);
        }
        if (body.expireAtEpochMs() <= sourcePosition.brokerPersistenceTimeEpochMs()) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.INVALID_DELIVERY_WINDOW);
        }
        final ShardQuota nextQuota;
        try {
            final long accountedBytes = Math.addExact(quota.pendingBytes(), quota.reservationBytes());
            final long accountedMessages = Math.addExact(quota.pendingMessages(), quota.reservationMessages());
            if (accountedMessages >= config.maxPendingMessages()
                    || current.payloadLength() > config.maxPendingBytes() - accountedBytes) {
                return persistSystemResult(
                        mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.HARD_QUOTA_EXCEEDED);
            }
            nextQuota = quota.addSchedule(current.payloadLength(), false);
        } catch (ArithmeticException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.HARD_QUOTA_EXCEEDED);
        }
        final LaneQuotaUsageProjection nextLaneQuota = laneQuotaUsage.addSchedule(
                current.laneId(),
                lane.laneIncarnation(),
                current.payloadLength(),
                false,
                Math.max(1, nextQuota.usageRevision()));
        final int nextGeneration;
        try {
            nextGeneration = UnsignedInt32.successor(current.generation());
        } catch (ArithmeticException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
        final long actionAt = actionAtFor(body.messageId(), current, body.deliverAtEpochMs());
        MessageRecord next = MessageRecord.current(
                MessageStatus.SCHEDULED,
                nextGeneration,
                Math.addExact(current.stateVersion(), 1),
                body.deliverAtEpochMs(),
                body.expireAtEpochMs(),
                current.laneId(),
                current.orderingMode(),
                current.payload(),
                sourcePosition.canonicalBytes(),
                current.payloadReference(),
                actionAt);
        next = next.withRuntimeIndex(timelineRuntimeIndex(
                body.messageId(),
                next,
                TimelineWorkKind.INITIAL_SCHEDULE,
                1,
                next.stateVersion(),
                UncertainRetryAuthority.NONE,
                null,
                null));
        final MessageRecord nextForWrite = next;
        final Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections =
                readyProjections(sourcePosition, body.messageId(), current, next, null, nextLaneQuota);
        final SystemMutationResult result = SystemMutationResult.from(
                mutation, ApplyStatus.APPLIED, StableCode.OK, sourcePosition.canonicalBytes());
        store.write(batch -> {
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(body.messageId()), nextForWrite.encode());
            batch.putValue(
                    ColumnFamily.TIMELINE,
                    1,
                    timelineKey(body.messageId(), nextForWrite),
                    encodeTimelineValue(body.messageId(), nextForWrite));
            batch.putValue(
                    ColumnFamily.TIMELINE,
                    1,
                    expiryKey(body.messageId(), nextForWrite),
                    encodeTimelineValue(body.messageId(), nextForWrite));
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            persistQuota(batch, nextQuota, nextLaneQuota);
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        quota = nextQuota;
        laneQuotaUsage = nextLaneQuota;
        return result;
    }

    /**
     * Applies the replay-stable CLAIM_RESULT subset. A locally persisted
     * Claim is consumed by exact precondition/instance identity; after replay,
     * the source-derived SCHEDULED fallback remains accepted when the Claim
     * record itself was not present in the restored checkpoint. The local
     * GenerationRuntimeIndex and obligation-set fences are checked before
     * terminalization; external materialization/recovery and grant authority
     * remain separate release work. This never treats a callback as a direct
     * terminal write: the result, terminal projection, quota transfer, indexes,
     * and source position share one synchronous batch.
     */
    private SystemMutationResult applyClaimResultMutation(
            final SystemMutation mutation, final SourcePosition sourcePosition) {
        final ClaimResultBody body = ClaimResultBody.decode(mutation.canonicalBody());
        if (!Bytes.constantTimeEquals(mutation.logicalOperationIdentity(), body.claimId())) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final com.nereusstream.delay.protocol.AuthorIdentity author =
                com.nereusstream.delay.protocol.AuthorIdentity.decode(mutation.authorIdentity());
        if (!Arrays.equals(
                author.asOwnerIdentity().canonicalBytes(), body.precondition().ownerIdentity())) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }

        final DelayMessageId messageId = new DelayMessageId(body.messageId());
        final MessageRecord current = getMessage(messageId);
        if (current == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.NOT_FOUND);
        }
        if (current.generation() != body.generation()) {
            final StableCode code = compareGeneration(current.generation(), body.generation()) > 0
                    ? StableCode.GENERATION_SUPERSEDED
                    : StableCode.STALE_SYSTEM_MUTATION;
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, code);
        }
        if (current.status() != MessageStatus.SCHEDULED && current.status() != MessageStatus.CLAIMED) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }

        final ClaimResultBody.ClaimPrecondition precondition = body.precondition();
        final ClaimRecord currentClaim;
        final byte[] sourceTimelineKey;
        if (current.status() == MessageStatus.CLAIMED) {
            currentClaim = getClaim(body.claimId(), author.generation());
            if (currentClaim == null
                    || !Arrays.equals(currentClaim.preconditionBytes(), precondition.canonicalBytes())
                    || currentClaim.runtimeRevision() != current.stateVersion()) {
                return persistSystemResult(
                        mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
            }
            sourceTimelineKey = currentClaim.timelineKey();
        } else {
            currentClaim = null;
            sourceTimelineKey = timelineKey(messageId, current);
        }
        final LaneRecord lane = readLane(current.laneId());
        if (lane == null) {
            throw new IllegalStateException("Claim Result references a missing Lane");
        }
        if (!lane.laneId().equals(current.laneId())
                || !Arrays.equals(lane.laneIncarnation(), precondition.laneIncarnation())
                || lane.laneControlVersion() != precondition.laneControlVersion()
                || (current.status() == MessageStatus.CLAIMED
                        ? current.stateVersion() != Math.addExact(precondition.stateVersion(), 1)
                        : current.stateVersion() != precondition.stateVersion())
                || !Arrays.equals(current.laneId().bytes(), precondition.destinationLaneId())
                || !Arrays.equals(Bytes.sha256(sourceTimelineKey), precondition.originalTimelineKeySha256())) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }

        final int expectedWorkKind = currentClaim != null
                ? ClaimResultBody.decodePrecondition(currentClaim.preconditionBytes())
                        .sourceWorkKind()
                : current.runtimeIndex().timeline() == null
                        ? (current.retryEligibilityAtEpochMs() == current.deliverAtEpochMs() ? 1 : 2)
                        : current.runtimeIndex().timeline().workKind().wireValue();
        final byte[] expectedSemanticDigest = currentClaim != null
                ? precondition.sourceTimelineSemanticDigest()
                : current.runtimeIndex().timeline() == null
                        ? timelineRuntimeIndex(
                                        messageId,
                                        current,
                                        expectedWorkKind == 1
                                                ? TimelineWorkKind.INITIAL_SCHEDULE
                                                : TimelineWorkKind.DEFINITIVE_RETRY,
                                        Math.addExact(current.runtimeIndex().admissionsUsed(), 1),
                                        current.stateVersion(),
                                        UncertainRetryAuthority.NONE,
                                        null,
                                        null,
                                        current.runtimeIndex())
                                .timeline()
                                .semanticWorkDigest()
                        : current.runtimeIndex().timeline().semanticWorkDigest();
        if (precondition.sourceWorkKind() != expectedWorkKind
                || precondition.expectedAdmissionsUsed()
                        != current.runtimeIndex().admissionsUsed()
                || precondition.expectedUncertainRetryAdmissionsUsed()
                        != current.runtimeIndex().uncertainRetryAdmissionsUsed()
                || !Bytes.constantTimeEquals(
                        precondition.expectedObligationSetDigest(),
                        GenerationRuntimeIndex.obligationSetDigest(
                                current.runtimeIndex().attemptObligations()))
                || !Bytes.constantTimeEquals(precondition.sourceTimelineSemanticDigest(), expectedSemanticDigest)) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }

        MessageRecord terminalMessage = MessageRecord.current(
                MessageStatus.DEAD_LETTER,
                current.generation(),
                Math.addExact(current.stateVersion(), 1),
                current.deliverAtEpochMs(),
                current.expireAtEpochMs(),
                current.laneId(),
                current.orderingMode(),
                current.payload(),
                current.scheduleSourcePosition(),
                current.payloadReference(),
                current.retryEligibilityAtEpochMs());
        terminalMessage = terminalMessage.withRuntimeIndex(GenerationRuntimeIndex.none(
                GenerationAggregateState.DEAD_LETTER,
                current.runtimeIndex().attemptObligations(),
                current.runtimeIndex().admissionsUsed(),
                current.runtimeIndex().uncertainRetryAdmissionsUsed(),
                current.runtimeIndex().possibleDestinationDuplicate(),
                terminalMessage.stateVersion()));
        final TerminalGenerationRecord terminal = new TerminalGenerationRecord(
                messageId,
                body.generation(),
                MessageStatus.DEAD_LETTER,
                StableCode.CLAIM_PERMANENT_FAILURE,
                terminalMessage.stateVersion(),
                sourcePosition.canonicalBytes(),
                terminalMessage.runtimeIndex().possibleDestinationDuplicate(),
                terminalMessage.runtimeIndex().attemptObligations());
        final DlqExportRecord dlqExport = DlqExportRecord.notConfigured(
                messageId, body.generation(), terminalMessage.stateVersion(), sourcePosition.canonicalBytes());
        final ShardQuota nextQuota;
        try {
            nextQuota = quota.removeSchedule(current.payloadLength());
        } catch (IllegalStateException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.INTEGRITY_ERROR);
        }
        LaneQuotaUsageProjection nextLaneQuota = removeScheduleQuotaUsage(current, nextQuota);
        if (currentClaim != null) {
            nextLaneQuota = mutateInflightQuotaUsage(
                    nextLaneQuota,
                    currentClaim.laneId(),
                    currentClaim.laneIncarnation(),
                    claimCharge(currentClaim),
                    false,
                    Math.max(1, nextQuota.usageRevision()));
        }
        final LaneQuotaUsageProjection projectedLaneQuota = nextLaneQuota;
        final Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections =
                readyProjections(sourcePosition, messageId, current, terminalMessage, null, projectedLaneQuota);
        final SystemMutationResult result = SystemMutationResult.from(
                mutation, ApplyStatus.APPLIED, StableCode.CLAIM_PERMANENT_FAILURE, sourcePosition.canonicalBytes());
        final MessageRecord terminalMessageForWrite = terminalMessage;
        store.write(batch -> {
            batch.delete(ColumnFamily.TIMELINE, sourceTimelineKey);
            batch.delete(ColumnFamily.TIMELINE, expiryKey(messageId, current));
            if (currentClaim != null) {
                batch.delete(ColumnFamily.INFLIGHT, currentClaim.encodedKey());
            }
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(messageId), terminalMessageForWrite.encode());
            batch.putValue(
                    ColumnFamily.TERMINAL,
                    1,
                    KeyCodec.terminalGeneration(messageId, body.generation()),
                    terminal.encode());
            batch.putValue(
                    ColumnFamily.TERMINAL,
                    DlqExportRecord.VALUE_TYPE,
                    KeyCodec.terminalDlqExport(dlqExport.dlqExportId()),
                    dlqExport.encode());
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            persistQuota(batch, nextQuota, projectedLaneQuota);
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        quota = nextQuota;
        laneQuotaUsage = projectedLaneQuota;
        return result;
    }

    /**
     * Validates the replay-stable portion of a source-ordered Publish Admission.
     *
     * <p>The local Claim and its runtime instance are useful optimizations, but
     * neither is the source of truth after checkpoint/replay. The signed body
     * must therefore still match the current Message/Lane projection and the
     * generation runtime counters before a new PUBLISHING obligation is made
     * durable.</p>
     */
    private AdmissionReplayState validatePublishAdmissionReplayState(
            final PublishAdmissionBody body,
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
                || !Arrays.equals(
                        precondition.destinationLaneId(), current.laneId().bytes())
                || !Arrays.equals(precondition.laneIncarnation(), lane.laneIncarnation())
                || precondition.laneControlVersion() != lane.laneControlVersion()) {
            throw new IllegalStateException("Publish Admission source identity is stale");
        }
        final long expectedStateVersion = current.status() == MessageStatus.CLAIMED
                ? Math.addExact(precondition.stateVersion(), 1)
                : precondition.stateVersion();
        if (current.stateVersion() != expectedStateVersion) {
            throw new IllegalStateException("Publish Admission message state version is stale");
        }
        final byte[] sourceTimelineKey =
                localClaim == null ? timelineKey(messageId, current) : localClaim.timelineKey();
        if (!Bytes.constantTimeEquals(precondition.originalTimelineKeySha256(), Bytes.sha256(sourceTimelineKey))) {
            throw new IllegalStateException("Publish Admission timeline key projection is stale");
        }
        if (precondition.expectedAdmissionsUsed() != index.admissionsUsed()
                || precondition.expectedUncertainRetryAdmissionsUsed() != index.uncertainRetryAdmissionsUsed()
                || !Bytes.constantTimeEquals(
                        precondition.expectedObligationSetDigest(),
                        GenerationRuntimeIndex.obligationSetDigest(index.attemptObligations()))) {
            throw new IllegalStateException("Publish Admission runtime counters are stale");
        }
        final int expectedAttemptNo = Math.addExact(index.admissionsUsed(), 1);
        if (body.descriptor().attemptNo() != expectedAttemptNo) {
            throw new IllegalStateException("Publish Admission attempt number is not replay-stable");
        }
        if (current.status() == MessageStatus.SCHEDULED && index.currentWorkKind() != CurrentSendWorkKind.TIMELINE) {
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
            sourceWork = new TimelineWorkRef(
                    sourceWorkKind,
                    sourceTimelineKey,
                    current.deliverAtEpochMs(),
                    current.retryEligibilityAtEpochMs(),
                    expectedAttemptNo,
                    Math.max(1, index.runtimeRevision()),
                    current.orderingMode() == com.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO,
                    UncertainRetryAuthority.NONE,
                    null,
                    null);
        }
        if (localClaim == null
                && (sourceWork == null
                        || sourceWork.workKind() != sourceWorkKind
                        || !Bytes.constantTimeEquals(
                                sourceWork.semanticWorkDigest(), precondition.sourceTimelineSemanticDigest()))) {
            throw new IllegalStateException("Publish Admission timeline semantic digest is stale");
        }
        if (sourceWorkKind == TimelineWorkKind.DEFINITIVE_RETRY
                && !index.attemptObligations().isEmpty()) {
            throw new IllegalStateException("definitive retry cannot carry open attempt obligations");
        }
        final boolean uncertainRetry =
                index.attemptObligations().stream().anyMatch(ref -> ref.ledgerState() == AttemptLedgerState.UNCERTAIN);
        if (uncertainRetry) {
            if (sourceWorkKind != TimelineWorkKind.UNCERTAIN_RETRY
                    || current.orderingMode() != com.nereusstream.delay.protocol.OrderingMode.BEST_EFFORT) {
                throw new IllegalStateException("older UNCERTAIN obligation requires an uncertain retry work item");
            }
        } else if (sourceWorkKind == TimelineWorkKind.UNCERTAIN_RETRY) {
            throw new IllegalStateException("UNCERTAIN_RETRY has no older UNCERTAIN obligation");
        }
        validateAdmissionBudget(new DelayMessageId(body.messageId()), current, index, uncertainRetry, sourcePosition);
        return new AdmissionReplayState(localClaim == null, uncertainRetry);
    }

    private void validateAdmissionBudget(
            final DelayMessageId messageId,
            final MessageRecord current,
            final GenerationRuntimeIndex index,
            final boolean uncertainRetryAdmission,
            final SourcePosition sourcePosition) {
        final RetryPolicySemantic policy = retryPolicyFor(messageId, current, sourcePosition);
        final int maxPublishAdmissions = policy == null ? config.maxPublishAdmissions() : policy.maxPublishAdmissions();
        final int maxUncertainRetries = policy == null ? config.maxUncertainRetries() : policy.maxUncertainRetries();
        if (index.admissionsUsed() >= maxPublishAdmissions) {
            throw new IllegalStateException("generation publish admission budget is exhausted");
        }
        if (uncertainRetryAdmission && index.uncertainRetryAdmissionsUsed() >= maxUncertainRetries) {
            throw new IllegalStateException("generation uncertain-retry admission budget is exhausted");
        }
    }

    private MessageRecord normalizeCommandRuntime(
            final DelayMessageId messageId,
            final MessageRecord prior,
            final MessageRecord next,
            final CommandResult result) {
        if (next == null) {
            return null;
        }
        if (next.status() == MessageStatus.SCHEDULED) {
            final TimelineWorkKind kind = TimelineWorkKind.INITIAL_SCHEDULE;
            if (result.stableCode() == StableCode.SUPERSEDED && prior != null) {
                // Reschedule creates a new generation but must retain the
                // prior generation's pinned action boundary when the
                // deliverAt is unchanged (and must re-derive the same pinned
                // Profile handoff boundary when it changes). Rebuilding
                // from `next` alone would lose that source projection because
                // its compatibility runtime index is intentionally empty.
                return next.withRuntimeIndex(rescheduledTimelineRuntimeIndex(messageId, prior, next));
            }
            return next.withRuntimeIndex(timelineRuntimeIndex(
                    messageId, next, kind, 1, next.stateVersion(), UncertainRetryAuthority.NONE, null, null));
        }
        if (prior != null && isTerminalStatus(next.status())) {
            return next.withRuntimeIndex(GenerationRuntimeIndex.none(
                    GenerationAggregateState.fromMessageStatus(next.status()),
                    prior.runtimeIndex().attemptObligations(),
                    prior.runtimeIndex().admissionsUsed(),
                    prior.runtimeIndex().uncertainRetryAdmissionsUsed(),
                    prior.runtimeIndex().possibleDestinationDuplicate(),
                    next.stateVersion()));
        }
        return next.withRuntimeIndex(GenerationRuntimeIndex.none(
                GenerationAggregateState.fromMessageStatus(next.status()),
                List.of(),
                0,
                0,
                false,
                Math.max(1, next.stateVersion())));
    }

    private GenerationRuntimeIndex rescheduledTimelineRuntimeIndex(
            final DelayMessageId messageId, final MessageRecord prior, final MessageRecord next) {
        final long actionAt = actionAtFor(messageId, prior, next.deliverAtEpochMs());
        final byte[] key = timelineKey(messageId, next, actionAt);
        final TimelineWorkRef work = TimelineWorkRef.initial(key, actionAt, Math.max(1, next.stateVersion()));
        return GenerationRuntimeIndex.timeline(
                GenerationAggregateState.SCHEDULED, work, List.of(), 0, 0, false, Math.max(1, next.stateVersion()));
    }

    private static boolean isTerminalStatus(final MessageStatus status) {
        return status == MessageStatus.CANCELED
                || status == MessageStatus.SUPERSEDED
                || status == MessageStatus.PUBLISHED
                || status == MessageStatus.HANDED_OFF
                || status == MessageStatus.EXPIRED
                || status == MessageStatus.DEAD_LETTER;
    }

    /**
     * Selects the success terminal projection from the immutable Admission
     * timing and the verified evidence branch. The wire outcome intentionally
     * keeps {@code PublishSideEffect.PUBLISHED} as the only success branch;
     * {@code HANDED_OFF} is a local aggregate projection for a certified
     * Pulsar handoff (fixed early {@code actionAt} plus a Broker send ACK).
     */
    private static MessageStatus publishedStatusForOutcome(
            final PublishAttemptLedger ledger, final PublishOutcomeBody outcome) {
        return publishedStatusForEvidence(outcome.evidence(), ledger);
    }

    private static MessageStatus publishedStatusForEvidence(final byte[] evidence, final PublishAttemptLedger ledger) {
        if (evidence == null || evidence.length == 0) {
            // The direct embedded helper has no wire evidence and represents
            // the historical ordinary-managed success API.
            return MessageStatus.PUBLISHED;
        }
        final PublishEvidence proof = PublishEvidence.decode(evidence);
        final PublishAdmissionBody admission;
        try {
            admission = PublishAdmissionBody.decode(ledger.admissionBytes());
        } catch (IllegalArgumentException legacyAdmission) {
            // Older embedded fixtures retained an opaque admission token and
            // therefore cannot prove a timing-policy handoff. Preserve their
            // ordinary PUBLISHED compatibility projection; a bytes-shaped
            // canonical Admission must still fail closed when malformed.
            final byte[] encoded = ledger.admissionBytes();
            if (encoded.length != 0 && (encoded[0] & 0xff) == 0x0a) {
                throw legacyAdmission;
            }
            return MessageStatus.PUBLISHED;
        }
        final long actionAt = admission.descriptor().actionAtEpochMs();
        final long deliverAt = admission.descriptor().deliverAtEpochMs();
        if (actionAt == deliverAt) {
            return MessageStatus.PUBLISHED;
        }
        final ChannelResourceIdentity channel =
                ChannelResourceIdentity.decode(admission.channel().canonicalBytes());
        if (actionAt < deliverAt
                && channel.adapterKind() == com.nereusstream.delay.protocol.AdapterKind.PULSAR
                && proof.evidenceKind() == PublishEvidenceKind.PULSAR_SEND_ACK) {
            proof.requireCertifiedPulsarHandoffBinding(admission);
            return MessageStatus.HANDED_OFF;
        }
        throw new IllegalArgumentException("early Publish Outcome lacks certified Pulsar handoff evidence");
    }

    private static boolean isTerminalAggregateState(final GenerationAggregateState state) {
        return switch (state) {
            case PUBLISHED, HANDED_OFF, CANCELED, EXPIRED, DEAD_LETTER, SUPERSEDED -> true;
            default -> false;
        };
    }

    private static PayloadAvailability payloadAvailability(final MessageRecord message) {
        return message.payloadReference() == null
                ? PayloadAvailability.INLINE_RETAINED
                : PayloadAvailability.OBJECT_RETAINED;
    }

    private static boolean hasUncertainObligation(final GenerationRuntimeIndex index) {
        return index.attemptObligations().stream()
                .anyMatch(obligation -> obligation.ledgerState() == AttemptLedgerState.UNCERTAIN);
    }

    private GenerationRuntimeIndex timelineRuntimeIndex(
            final DelayMessageId messageId,
            final MessageRecord message,
            final TimelineWorkKind workKind,
            final int candidateAttemptNo,
            final long runtimeRevision,
            final UncertainRetryAuthority authority,
            final byte[] control,
            final byte[] controlPosition) {
        return timelineRuntimeIndex(
                messageId,
                message,
                workKind,
                candidateAttemptNo,
                runtimeRevision,
                authority,
                control,
                controlPosition,
                null,
                null);
    }

    private GenerationRuntimeIndex timelineRuntimeIndex(
            final DelayMessageId messageId,
            final MessageRecord message,
            final TimelineWorkKind workKind,
            final int candidateAttemptNo,
            final long runtimeRevision,
            final UncertainRetryAuthority authority,
            final byte[] control,
            final byte[] controlPosition,
            final GenerationRuntimeIndex base) {
        return timelineRuntimeIndex(
                messageId,
                message,
                workKind,
                candidateAttemptNo,
                runtimeRevision,
                authority,
                control,
                controlPosition,
                base,
                base == null ? null : base.attemptObligations());
    }

    private GenerationRuntimeIndex timelineRuntimeIndex(
            final DelayMessageId messageId,
            final MessageRecord message,
            final TimelineWorkKind workKind,
            final int candidateAttemptNo,
            final long runtimeRevision,
            final UncertainRetryAuthority authority,
            final byte[] control,
            final byte[] controlPosition,
            final GenerationRuntimeIndex base,
            final List<AttemptObligationRef> obligations) {
        // Retry/rollback paths commonly construct a compatibility MessageRecord
        // first and then replace its runtime projection. That temporary record
        // has no TimelineWorkRef, so resolving from `message` alone would fall
        // back to deliverAt when the source-position resolver is not available
        // on the current process. The prior runtime projection is the durable
        // action boundary for the same generation and must be carried forward.
        final long actionAt = actionAtFor(messageId, message, base);
        final byte[] key = timelineKey(messageId, message, actionAt);
        final TimelineWorkRef work = new TimelineWorkRef(
                workKind,
                key,
                actionAt,
                message.retryEligibilityAtEpochMs(),
                candidateAttemptNo,
                runtimeRevision,
                message.orderingMode() == com.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO,
                authority,
                control,
                controlPosition);
        final GenerationAggregateState aggregate =
                switch (workKind) {
                    case INITIAL_SCHEDULE -> GenerationAggregateState.SCHEDULED;
                    case DEFINITIVE_RETRY -> GenerationAggregateState.RETRY_WAIT;
                    case UNCERTAIN_RETRY -> GenerationAggregateState.UNCERTAIN;
                };
        final List<AttemptObligationRef> retained = obligations == null ? List.of() : obligations;
        final int admissionsUsed = base == null ? 0 : base.admissionsUsed();
        final int uncertainRetryAdmissionsUsed = base == null ? 0 : base.uncertainRetryAdmissionsUsed();
        final boolean possibleDestinationDuplicate = base != null && base.possibleDestinationDuplicate();
        return GenerationRuntimeIndex.timeline(
                aggregate,
                work,
                retained,
                admissionsUsed,
                uncertainRetryAdmissionsUsed,
                possibleDestinationDuplicate,
                runtimeRevision);
    }

    /**
     * Resolves the durable action boundary without changing the business
     * meaning of {@code deliverAt}. The optional resolver projection is used
     * during the initial Schedule apply; later retries/recovery derive the
     * same value from the persisted Schedule binding. Legacy messages and
     * catalog-less embedded seams remain ordinary managed ({@code actionAt =
     * deliverAt}).
     */
    private long actionAtFor(final DelayMessageId messageId, final MessageRecord message) {
        return actionAtFor(messageId, message, message.deliverAtEpochMs());
    }

    private long actionAtFor(final DelayMessageId messageId, final MessageRecord message, final long deliverAtEpochMs) {
        return actionAtFor(messageId, message, deliverAtEpochMs, null);
    }

    private long actionAtFor(
            final DelayMessageId messageId, final MessageRecord message, final GenerationRuntimeIndex priorRuntime) {
        return actionAtFor(messageId, message, message.deliverAtEpochMs(), priorRuntime);
    }

    private long actionAtFor(
            final DelayMessageId messageId,
            final MessageRecord message,
            final long deliverAtEpochMs,
            final GenerationRuntimeIndex priorRuntime) {
        if (lastResolvedSchedule != null
                && lastResolvedScheduleMessageId != null
                && lastResolvedScheduleMessageId.equals(messageId)
                && lastResolvedSchedule.laneId().equals(message.laneId())
                && lastResolvedSchedule.actionAtEpochMs() != null) {
            return checkedActionAt(lastResolvedSchedule.actionAtEpochMs(), deliverAtEpochMs);
        }
        if (priorRuntime != null
                && priorRuntime.timeline() != null
                && priorRuntime.timeline().actionAtEpochMs() <= deliverAtEpochMs) {
            return priorRuntime.timeline().actionAtEpochMs();
        }
        final TimelineWorkRef existing = message.runtimeIndex().timeline();
        if (existing != null
                && message.deliverAtEpochMs() == deliverAtEpochMs
                && existing.actionAtEpochMs() <= deliverAtEpochMs) {
            return existing.actionAtEpochMs();
        }
        final Long claimedActionAt = actionAtFromLiveClaim(messageId, message, deliverAtEpochMs);
        if (claimedActionAt != null) {
            return claimedActionAt;
        }
        final Long admittedActionAt = actionAtFromOpenAdmission(messageId, message, deliverAtEpochMs);
        if (admittedActionAt != null) {
            return admittedActionAt;
        }
        if (profileCatalog == null) {
            return deliverAtEpochMs;
        }
        final ScheduleBinding binding = getScheduleBinding(messageId);
        if (binding == null) {
            return deliverAtEpochMs;
        }
        final ProfileRef destinationRef = binding.commandType() == com.nereusstream.delay.protocol.CommandType.SCHEDULE
                ? CommandBodies.decodeSchedule(binding.canonicalBody()).intent().profile()
                : CommandBodies.decodePrepareLarge(binding.canonicalBody())
                        .intentWithoutPayload()
                        .profile();
        final ProfileSemanticEnvelope destination = profileCatalog.resolve(destinationRef);
        if (destination == null
                || !destination.ref().equals(destinationRef)
                || destination.profileKind() != com.nereusstream.delay.protocol.ProfileKind.DESTINATION
                || !(destination.body() instanceof DestinationProfileSemantic body)) {
            throw new CommandResolutionException(
                    StableCode.ROUTE_SNAPSHOT_UNAVAILABLE,
                    "pinned Destination Profile is unavailable during actionAt derivation");
        }
        final ProfileSemanticEnvelope capability = profileCatalog.resolve(body.deliveryCapability());
        if (capability == null
                || !capability.ref().equals(body.deliveryCapability())
                || capability.profileKind() != com.nereusstream.delay.protocol.ProfileKind.DELIVERY_CAPABILITY
                || !(capability.body() instanceof DeliveryCapabilitySemantic capabilityBody)
                || capabilityBody.adapterKind() != body.adapterKind()) {
            throw new CommandResolutionException(
                    StableCode.ROUTE_SNAPSHOT_UNAVAILABLE,
                    "pinned Delivery Capability is unavailable during actionAt derivation");
        }
        if (body.adapterKind() != com.nereusstream.delay.protocol.AdapterKind.PULSAR
                || body.handoffLeadMs() <= 0
                || !TimingCapability.includes(
                        capabilityBody.timingCapabilityBits(), TimingCapability.PULSAR_GUARDED_HANDOFF)) {
            return deliverAtEpochMs;
        }
        try {
            return checkedActionAt(Math.subtractExact(deliverAtEpochMs, body.handoffLeadMs()), deliverAtEpochMs);
        } catch (ArithmeticException overflow) {
            throw new IllegalStateException(" certified handoff actionAt arithmetic overflow", overflow);
        }
    }

    /**
     * Recovers the immutable action boundary retained by a live Claim. A
     * CLAIMED Message intentionally has no current TimelineWorkRef, and an
     * older open Admission may be opaque or may be the obligation currently
     * being settled; the Claim snapshot remains the exact local source for
     * rollback/retry projection in either case.
     */
    private Long actionAtFromLiveClaim(
            final DelayMessageId messageId, final MessageRecord message, final long deliverAtEpochMs) {
        if (message.status() != MessageStatus.CLAIMED
                && message.runtimeIndex().currentWorkKind() != CurrentSendWorkKind.CLAIMED) {
            return null;
        }
        final ClaimRecord claim = findClaimForMessage(messageId);
        if (claim == null || claim.generation() != message.generation()) {
            return null;
        }
        final byte[] sourceTimelineWork = claim.sourceTimelineWork();
        if (sourceTimelineWork.length == 0) {
            return null;
        }
        final TimelineWorkRef sourceWork = TimelineWorkRef.decode(sourceTimelineWork);
        if (!Arrays.equals(sourceWork.encodedTimelineKey(), claim.timelineKey())
                || sourceWork.retryEligibilityAtEpochMs() != message.retryEligibilityAtEpochMs()) {
            throw new IllegalStateException("live Claim source timeline does not match current Message");
        }
        return checkedActionAt(sourceWork.actionAtEpochMs(), deliverAtEpochMs);
    }

    /**
     * Recovers the pinned action boundary while the current generation is in
     * PUBLISHING/UNCERTAIN state. Those runtime branches intentionally carry
     * only the active attempt/obligation identity, while a canonical
     * Publish Admission retains the immutable descriptor that pinned actionAt.
     * Legacy opaque ledgers have no such evidence and remain on the ordinary
     * compatibility path.
     */
    private Long actionAtFromOpenAdmission(
            final DelayMessageId messageId, final MessageRecord message, final long deliverAtEpochMs) {
        Long resolved = null;
        for (final PublishAttemptLedger ledger : listOpenPublishAttempts()) {
            if (!ledger.delayMessageId().equals(messageId) || ledger.generation() != message.generation()) {
                continue;
            }
            final PublishAdmissionBody admission;
            try {
                admission = PublishAdmissionBody.decode(ledger.admissionBytes());
            } catch (IllegalArgumentException legacyOrMalformed) {
                failClosedForMalformedCanonicalAdmission(ledger.admissionBytes(), legacyOrMalformed);
                continue;
            }
            if (admission.descriptor().deliverAtEpochMs() != deliverAtEpochMs) {
                throw new IllegalStateException("open Admission timing does not match current Message");
            }
            final long candidate = checkedActionAt(admission.descriptor().actionAtEpochMs(), deliverAtEpochMs);
            if (resolved != null && resolved.longValue() != candidate) {
                throw new IllegalStateException("open Admissions disagree on the pinned actionAt");
            }
            resolved = candidate;
        }
        return resolved;
    }

    private static long checkedActionAt(final long actionAtEpochMs, final long deliverAtEpochMs) {
        if (actionAtEpochMs < 0 || actionAtEpochMs > deliverAtEpochMs) {
            throw new IllegalStateException(" actionAt is outside the deliverAt boundary");
        }
        return actionAtEpochMs;
    }

    private static List<AttemptObligationRef> withoutObligation(
            final GenerationRuntimeIndex index, final byte[] publishAttemptId) {
        return withoutObligation(index.attemptObligations(), publishAttemptId);
    }

    private static List<AttemptObligationRef> withoutObligation(
            final List<AttemptObligationRef> obligations, final byte[] publishAttemptId) {
        final List<AttemptObligationRef> result = new ArrayList<>();
        for (AttemptObligationRef ref : obligations) {
            if (!Arrays.equals(ref.publishAttemptId(), publishAttemptId)) {
                result.add(ref);
            }
        }
        result.sort(DelayShard::compareObligation);
        return result;
    }

    private static List<AttemptObligationRef> withObligation(
            final GenerationRuntimeIndex index, final AttemptObligationRef obligation) {
        return withObligation(index.attemptObligations(), obligation);
    }

    private static List<AttemptObligationRef> withObligation(
            final List<AttemptObligationRef> obligations, final AttemptObligationRef obligation) {
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

    private SystemMutationResult applyNotPublishedPublishOutcome(
            final PublishAttemptLedger ledger,
            final PublishOutcomeBody outcome,
            final SourcePosition sourcePosition,
            final SystemMutationResult systemResult,
            final AttemptLedgerState expectedLedgerState,
            final MessageStatus expectedMessageStatus) {
        requirePublishAttemptLane(ledger, "not-published outcome");
        final MessageRecord current = getMessage(ledger.delayMessageId());
        if (ledger.state() != expectedLedgerState
                || current == null
                || compareGeneration(current.generation(), ledger.generation()) < 0) {
            return persistSystemResultByResult(systemResult, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
        }
        if (compareGeneration(current.generation(), ledger.generation()) > 0) {
            final PublishOutcomeBody.RetryDecision retryDecision = outcome.retryDecision();
            if (retryDecision.completedAttemptNo() != UnsignedInt32.toLong(ledger.attemptNo())) {
                return persistSystemResultByResult(systemResult, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
            }
            settleHistoricalTerminalObligation(ledger, sourcePosition, systemResult, false);
            return systemResult;
        }
        if (isTerminalStatus(current.status())) {
            if (outcome.retryDecision().completedAttemptNo() != UnsignedInt32.toLong(ledger.attemptNo())) {
                return persistSystemResultByResult(systemResult, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
            }
            settleTerminalObligation(ledger, current, sourcePosition, systemResult, false);
            return systemResult;
        }
        if (current.status() != expectedMessageStatus) {
            return persistSystemResultByResult(systemResult, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
        }
        final PublishOutcomeBody.RetryDecision retryDecision = outcome.retryDecision();
        if (retryDecision.completedAttemptNo() != UnsignedInt32.toLong(ledger.attemptNo())
                || retryDecision.retryDeadline() > current.expireAtEpochMs()
                || retryDecision.firstAttemptAt() > retryDecision.retryDeadline()
                || retryDecision.hasNextRetryAt() && retryDecision.nextRetryAt() < current.deliverAtEpochMs()) {
            return persistSystemResultByResult(systemResult, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
        }
        final LaneRecord currentLane = readLane(current.laneId());
        final boolean closedAfterAdmission = currentLane != null && currentLane.admissionGate() == AdmissionGate.CLOSED;
        final StableCode terminalCode =
                closedAfterAdmission ? StableCode.LANE_CLOSED_AFTER_ADMISSION_NOT_PUBLISHED : outcome.stableCode();
        final SystemMutationResult terminalResult = closedAfterAdmission
                ? new SystemMutationResult(
                        systemResult.mutationId(),
                        systemResult.mutationHash(),
                        systemResult.mutationType(),
                        systemResult.retryUntilEpochMs(),
                        systemResult.authorIdentity(),
                        systemResult.applyStatus(),
                        terminalCode,
                        sourcePosition.canonicalBytes())
                : systemResult;
        if (outcome.disposition() == 2 || closedAfterAdmission) {
            MessageRecord terminalMessage = MessageRecord.current(
                    MessageStatus.DEAD_LETTER,
                    current.generation(),
                    Math.addExact(current.stateVersion(), 1),
                    current.deliverAtEpochMs(),
                    current.expireAtEpochMs(),
                    current.laneId(),
                    current.orderingMode(),
                    current.payload(),
                    current.scheduleSourcePosition(),
                    current.payloadReference(),
                    current.retryEligibilityAtEpochMs());
            terminalMessage = terminalMessage.withRuntimeIndex(GenerationRuntimeIndex.none(
                    GenerationAggregateState.DEAD_LETTER,
                    withoutObligation(current.runtimeIndex(), ledger.publishAttemptId()),
                    current.runtimeIndex().admissionsUsed(),
                    current.runtimeIndex().uncertainRetryAdmissionsUsed(),
                    current.runtimeIndex().possibleDestinationDuplicate(),
                    terminalMessage.stateVersion()));
            final TerminalGenerationRecord terminal = new TerminalGenerationRecord(
                    ledger.delayMessageId(),
                    ledger.generation(),
                    MessageStatus.DEAD_LETTER,
                    terminalCode,
                    terminalMessage.stateVersion(),
                    sourcePosition.canonicalBytes(),
                    terminalMessage.runtimeIndex().possibleDestinationDuplicate(),
                    terminalMessage.runtimeIndex().attemptObligations());
            final DlqExportRecord dlqExport = DlqExportRecord.notConfigured(
                    ledger.delayMessageId(),
                    ledger.generation(),
                    terminalMessage.stateVersion(),
                    sourcePosition.canonicalBytes());
            final ShardQuota nextQuota = quota.removeSchedule(current.payloadLength());
            LaneQuotaUsageProjection nextLaneQuota = removeScheduleQuotaUsage(current, nextQuota);
            nextLaneQuota = mutateInflightQuotaUsage(
                    nextLaneQuota,
                    ledger.laneId(),
                    ledger.laneIncarnation(),
                    attemptCharge(ledger),
                    false,
                    Math.max(1, nextQuota.usageRevision()));
            final LaneQuotaUsageProjection projectedLaneQuota = nextLaneQuota;
            final OutcomeReserveUsage nextOutcomeReserve = releasedOutcomeReserve(ledger);
            final CapacityVector nextOutcomeReserveVector = releasedOutcomeReserveVector(ledger);
            final Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections = readyProjections(
                    sourcePosition, ledger.delayMessageId(), current, terminalMessage, null, projectedLaneQuota);
            final MessageRecord terminalMessageForWrite = terminalMessage;
            store.write(batch -> {
                batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
                batch.putValue(
                        ColumnFamily.ID,
                        1,
                        KeyCodec.idMessage(ledger.delayMessageId()),
                        terminalMessageForWrite.encode());
                batch.putValue(
                        ColumnFamily.TERMINAL,
                        1,
                        KeyCodec.terminalGeneration(ledger.delayMessageId(), ledger.generation()),
                        terminal.encode());
                batch.putValue(
                        ColumnFamily.TERMINAL,
                        DlqExportRecord.VALUE_TYPE,
                        KeyCodec.terminalDlqExport(dlqExport.dlqExportId()),
                        dlqExport.encode());
                for (LaneProjection projection : projections.values()) {
                    deleteReadyKey(batch, projection.previousLane());
                    putReadyProjection(batch, projection);
                }
                persistQuota(batch, nextQuota, projectedLaneQuota);
                persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector, projectedLaneQuota);
                writeSystemResult(batch, terminalResult);
                writePosition(batch, sourcePosition);
            });
            lastAppliedSourcePosition = sourcePosition;
            mutationSequence = nextMutationSequence();
            quota = nextQuota;
            laneQuotaUsage = projectedLaneQuota;
            outcomeReserve = nextOutcomeReserve;
            outcomeReserveVector = nextOutcomeReserveVector;
            return terminalResult;
        }
        if (current.orderingMode() == com.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO
                || !retryDecision.hasNextRetryAt()) {
            return persistSystemResultByResult(systemResult, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
        }
        final long retryAt = Math.max(current.deliverAtEpochMs(), retryDecision.nextRetryAt());
        if (retryAt >= current.expireAtEpochMs()) {
            return persistSystemResultByResult(systemResult, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
        }
        MessageRecord scheduled = MessageRecord.current(
                MessageStatus.SCHEDULED,
                current.generation(),
                Math.addExact(current.stateVersion(), 1),
                current.deliverAtEpochMs(),
                current.expireAtEpochMs(),
                current.laneId(),
                current.orderingMode(),
                current.payload(),
                current.scheduleSourcePosition(),
                current.payloadReference(),
                retryAt);
        final List<AttemptObligationRef> remainingObligations =
                withoutObligation(current.runtimeIndex(), ledger.publishAttemptId());
        if (remainingObligations.stream().anyMatch(ref -> ref.ledgerState() == AttemptLedgerState.UNCERTAIN)) {
            throw new IllegalStateException("definitive retry cannot bypass an older UNCERTAIN obligation");
        }
        scheduled = scheduled.withRuntimeIndex(timelineRuntimeIndex(
                ledger.delayMessageId(),
                scheduled,
                TimelineWorkKind.DEFINITIVE_RETRY,
                UnsignedInt32.successor(ledger.attemptNo()),
                scheduled.stateVersion(),
                UncertainRetryAuthority.NONE,
                null,
                null,
                current.runtimeIndex(),
                remainingObligations));
        final MessageRecord scheduledForWrite = scheduled;
        final Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneRecord> laneOverrides = new HashMap<>();
        if (outcome.disposition() == 3) {
            final LaneRecord lane = readLane(current.laneId());
            if (lane == null) {
                return persistSystemResultByResult(systemResult, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
            }
            laneOverrides.put(current.laneId(), lane.withReadiness(RuntimeReadiness.BLOCKED));
        }
        final OutcomeReserveUsage nextOutcomeReserve = releasedOutcomeReserve(ledger);
        final CapacityVector nextOutcomeReserveVector = releasedOutcomeReserveVector(ledger);
        final LaneQuotaUsageProjection nextLaneQuota = removeAttemptQuotaUsage(ledger);
        final Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections = readyProjections(
                sourcePosition, ledger.delayMessageId(), current, scheduled, null, laneOverrides, nextLaneQuota);
        store.write(batch -> {
            batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(ledger.delayMessageId()), scheduledForWrite.encode());
            batch.putValue(
                    ColumnFamily.TIMELINE,
                    1,
                    timelineKey(ledger.delayMessageId(), scheduledForWrite),
                    encodeTimelineValue(ledger.delayMessageId(), scheduledForWrite));
            batch.putValue(
                    ColumnFamily.TIMELINE,
                    1,
                    expiryKey(ledger.delayMessageId(), scheduledForWrite),
                    encodeTimelineValue(ledger.delayMessageId(), scheduledForWrite));
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            persistQuota(batch, quota, nextLaneQuota);
            persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector, nextLaneQuota);
            writeSystemResult(batch, systemResult);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        laneQuotaUsage = nextLaneQuota;
        outcomeReserve = nextOutcomeReserve;
        outcomeReserveVector = nextOutcomeReserveVector;
        return systemResult;
    }

    private SystemMutationResult persistSystemResultByResult(
            final SystemMutationResult original, final SourcePosition sourcePosition, final StableCode code) {
        final SystemMutationResult result = new SystemMutationResult(
                original.mutationId(),
                original.mutationHash(),
                original.mutationType(),
                original.retryUntilEpochMs(),
                original.authorIdentity(),
                ApplyStatus.APPLIED,
                code,
                sourcePosition.canonicalBytes());
        store.write(batch -> {
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        return result;
    }

    private SystemMutationResult applyExpireGenerationMutation(
            final SystemMutation mutation, final SourcePosition sourcePosition) {
        final List<com.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field> fields =
                SystemMutationBodyCodec.fields(SystemMutationType.EXPIRE_GENERATION, mutation.canonicalBody());
        final DelayMessageId messageId =
                new DelayMessageId(fixedBodyBytes(field(fields, 10), 10, DelayMessageId.LENGTH));
        SystemMutationBodyCodec.requireMessageShard(fields, messageId, "Expire Generation");
        final int generation = bodyUint32Bits(field(fields, 11), 11);
        final long expireAt = bodyNonNegative(field(fields, 12), 12);
        if (!Bytes.constantTimeEquals(
                mutation.logicalOperationIdentity(),
                SystemMutation.computeExpiryLogicalIdentity(messageId, generation, expireAt))) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final TrustedUtcIntervalEvidence proof = TrustedUtcIntervalEvidence.decode(bytesBody(field(fields, 13), 13));
        proof.requireEarliestAtLeast(expireAt);
        final MessageRecord current = getMessage(messageId);
        if (current == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.NOT_FOUND);
        }
        if (current.generation() != generation) {
            final StableCode code = compareGeneration(current.generation(), generation) > 0
                    ? StableCode.GENERATION_SUPERSEDED
                    : StableCode.STALE_SYSTEM_MUTATION;
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, code);
        }
        if (current.expireAtEpochMs() != expireAt) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
        final LaneRecord lane = readLane(current.laneId());
        if (lane == null) {
            throw new IllegalStateException("Expire Generation references a missing Lane");
        }
        if (lane.admissionGate() == AdmissionGate.CLOSED && isUnadmittedGeneration(current)) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.LANE_CLOSED_BEFORE_ADMISSION);
        }
        if (current.status() == MessageStatus.EXPIRED) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.ALREADY_EXPIRED);
        }
        if (current.status() != MessageStatus.SCHEDULED && current.status() != MessageStatus.CLAIMED) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }
        final ClaimRecord claim = current.status() == MessageStatus.CLAIMED ? findClaimForMessage(messageId) : null;
        if (current.status() == MessageStatus.CLAIMED && claim == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.INTEGRITY_ERROR);
        }
        MessageRecord next = MessageRecord.current(
                MessageStatus.EXPIRED,
                current.generation(),
                Math.addExact(current.stateVersion(), 1),
                current.deliverAtEpochMs(),
                current.expireAtEpochMs(),
                current.laneId(),
                current.orderingMode(),
                current.payload(),
                current.scheduleSourcePosition(),
                current.payloadReference(),
                current.retryEligibilityAtEpochMs());
        next = next.withRuntimeIndex(GenerationRuntimeIndex.none(
                GenerationAggregateState.EXPIRED,
                current.runtimeIndex().attemptObligations(),
                current.runtimeIndex().admissionsUsed(),
                current.runtimeIndex().uncertainRetryAdmissionsUsed(),
                current.runtimeIndex().possibleDestinationDuplicate(),
                next.stateVersion()));
        final MessageRecord expiredNext = next;
        final TerminalGenerationRecord terminal = new TerminalGenerationRecord(
                messageId,
                generation,
                MessageStatus.EXPIRED,
                StableCode.ALREADY_EXPIRED,
                next.stateVersion(),
                sourcePosition.canonicalBytes(),
                next.runtimeIndex().possibleDestinationDuplicate(),
                next.runtimeIndex().attemptObligations());
        final ShardQuota nextQuota = quota.removeSchedule(current.payloadLength());
        LaneQuotaUsageProjection nextLaneQuota = removeScheduleQuotaUsage(current, nextQuota);
        if (claim != null) {
            nextLaneQuota = mutateInflightQuotaUsage(
                    nextLaneQuota,
                    claim.laneId(),
                    claim.laneIncarnation(),
                    claimCharge(claim),
                    false,
                    Math.max(1, nextQuota.usageRevision()));
        }
        final LaneQuotaUsageProjection projectedLaneQuota = nextLaneQuota;
        final Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections =
                readyProjections(sourcePosition, messageId, current, next, null, projectedLaneQuota);
        final SystemMutationResult result = SystemMutationResult.from(
                mutation, ApplyStatus.APPLIED, StableCode.OK, sourcePosition.canonicalBytes());
        store.write(batch -> {
            batch.delete(ColumnFamily.TIMELINE, claim == null ? timelineKey(messageId, current) : claim.timelineKey());
            batch.delete(ColumnFamily.TIMELINE, expiryKey(messageId, current));
            if (claim != null) {
                batch.delete(ColumnFamily.INFLIGHT, claim.encodedKey());
            }
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(messageId), expiredNext.encode());
            batch.putValue(
                    ColumnFamily.TERMINAL, 1, KeyCodec.terminalGeneration(messageId, generation), terminal.encode());
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            persistQuota(batch, nextQuota, projectedLaneQuota);
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        quota = nextQuota;
        laneQuotaUsage = projectedLaneQuota;
        return result;
    }

    /**
     * Applies the local, source-ordered DLQ export outbox transition. The
     * external target/evidence adapter is deliberately outside this method;
     * only a signed result mutation may move the durable export state.
     */
    private SystemMutationResult applyDlqExportResultMutation(
            final SystemMutation mutation, final SourcePosition sourcePosition) {
        final DlqExportResultBody body = DlqExportResultBody.decode(mutation.canonicalBody());
        if (!Bytes.constantTimeEquals(mutation.logicalOperationIdentity(), body.logicalOperationIdentity())) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final DelayMessageId messageId = new DelayMessageId(body.messageId());
        final TerminalGenerationRecord terminal = getTerminalGeneration(messageId, body.generation());
        if (terminal == null
                || terminal.status() != MessageStatus.DEAD_LETTER
                || terminal.stateVersion() != body.terminalRevision()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
        final DlqExportRecord current = getDlqExportRecord(messageId, body.generation());
        if (current == null
                || current.state() == DlqExportState.NOT_CONFIGURED
                || !Bytes.constantTimeEquals(current.exportEnvelopeHash(), body.exportEnvelopeHash())
                || !Bytes.constantTimeEquals(current.dlqExportId(), body.dlqExportId())) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.STALE_SYSTEM_MUTATION);
        }
        // The outbox retains the exact policy-derived charge at creation time;
        // a callback may not manufacture a different quota authority.
        if (!Bytes.constantTimeEquals(current.retainedCharge(), body.transfer())) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.STALE_SYSTEM_MUTATION);
        }
        try {
            validateDlqRetryDecision(body, messageId, getMessage(messageId), terminal, sourcePosition);
            validateDlqExportAttempt(current, body);
            final int nextAttempt = body.resultingState() == DlqExportState.PENDING
                    ? UnsignedInt32.successor(body.physicalAttemptNo())
                    : body.physicalAttemptNo();
            final DlqExportRecord next = new DlqExportRecord(
                    current.dlqExportId(),
                    messageId,
                    current.generation(),
                    current.terminalRevision(),
                    current.exportEnvelopeHash(),
                    current.retainedCharge(),
                    body.resultingState(),
                    nextAttempt,
                    sourcePosition.canonicalBytes());
            final SystemMutationResult result = SystemMutationResult.from(
                    mutation, ApplyStatus.APPLIED, body.stableCode(), sourcePosition.canonicalBytes());
            store.write(batch -> {
                batch.putValue(
                        ColumnFamily.TERMINAL,
                        DlqExportRecord.VALUE_TYPE,
                        KeyCodec.terminalDlqExport(next.dlqExportId()),
                        next.encode());
                writeSystemResult(batch, result);
                writePosition(batch, sourcePosition);
            });
            lastAppliedSourcePosition = sourcePosition;
            mutationSequence = nextMutationSequence();
            return result;
        } catch (IllegalStateException | IllegalArgumentException | ArithmeticException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.INTEGRITY_ERROR);
        }
    }

    /**
     * Recomputes the DLQ retry domain when the shard has the immutable
     * source-position policy catalog for this binding. Legacy records and
     * catalog-less compatibility shards retain structural validation only.
     */
    private void validateDlqRetryDecision(
            final DlqExportResultBody body,
            final DelayMessageId messageId,
            final MessageRecord current,
            final TerminalGenerationRecord terminal,
            final SourcePosition sourcePosition) {
        if (retryPolicyCatalog == null || current == null || getScheduleBinding(messageId) == null) {
            return;
        }
        final RetryPolicySemantic policy = retryPolicyFor(messageId, current, sourcePosition);
        if (policy == null || policy.dlqExportMode() == com.nereusstream.delay.protocol.DlqExportMode.NOT_CONFIGURED) {
            throw new IllegalStateException("DLQ export result has no enabled pinned DLQ policy");
        }
        final DlqExportResultBody.RetryDecision decision = body.parsedRetryDecision();
        if (decision.retryDomain() != RetryJitter.DLQ_EXPORT
                || !decision.policy().matches(policy)) {
            throw new IllegalStateException("DLQ RetryDecision does not match the pinned Retry Policy");
        }
        final SourcePosition terminalPosition = SourcePositionCodec.decode(terminal.appliedSourcePosition());
        final long firstExportAt = terminalPosition.brokerPersistenceTimeEpochMs();
        if (decision.firstAttemptAt() != firstExportAt) {
            throw new IllegalStateException("DLQ RetryDecision first attempt does not match terminalization time");
        }
        final long expectedDeadline = Math.addExact(firstExportAt, policy.dlqMaxRetryDurationMs());
        if (decision.retryDeadline() != expectedDeadline) {
            throw new IllegalStateException("DLQ RetryDecision deadline does not match the pinned policy");
        }
        final long completedAttemptNo = decision.completedAttemptNo();
        final long physicalAttemptNo = UnsignedInt32.toLong(body.physicalAttemptNo());
        if (completedAttemptNo <= 0
                || completedAttemptNo != physicalAttemptNo
                || completedAttemptNo > policy.dlqMaxAttempts()) {
            throw new IllegalStateException("DLQ RetryDecision attempt does not match the export policy");
        }
        if (body.resultingState() == DlqExportState.PENDING) {
            if (body.sideEffect() == 3 && !policy.dlqAllowPossibleDuplicate()) {
                throw new IllegalStateException("DLQ unknown retry requires possible-duplicate policy permission");
            }
            if (completedAttemptNo >= policy.dlqMaxAttempts() || !decision.hasNextRetryAt()) {
                throw new IllegalStateException("DLQ scheduled retry exceeds the pinned attempt budget");
            }
            final long cap = policy.dlqRetryBackoffCap(completedAttemptNo);
            final long jitter = RetryJitter.delayMs(
                    RetryJitter.DLQ_EXPORT,
                    messageId,
                    UnsignedInt32.toLong(body.generation()),
                    completedAttemptNo,
                    cap);
            final long expectedNext = Math.addExact(body.observedAt().latestEpochMs(), jitter);
            if (decision.nextRetryAt() != expectedNext || expectedNext > expectedDeadline) {
                throw new IllegalStateException("DLQ RetryDecision next retry does not match deterministic jitter");
            }
        }
    }

    private static void validateDlqExportAttempt(final DlqExportRecord current, final DlqExportResultBody body) {
        if (body.eventKind() == 1) {
            if (current.state() == DlqExportState.PUBLISHED || current.state() == DlqExportState.FAILED_PERMANENT) {
                throw new IllegalStateException("terminal DLQ export cannot accept another attempt outcome");
            }
            final int expectedAttempt = current.state() == DlqExportState.UNCERTAIN
                    ? UnsignedInt32.successor(current.physicalAttemptNo())
                    : current.physicalAttemptNo();
            if (body.physicalAttemptNo() != expectedAttempt) {
                throw new IllegalStateException("DLQ export attempt number is not the checked successor");
            }
        } else {
            if (current.state() != DlqExportState.UNCERTAIN && current.state() != DlqExportState.PENDING) {
                throw new IllegalStateException("evidence resolution has no open DLQ export state");
            }
            if (UnsignedInt32.compare(body.physicalAttemptNo(), current.physicalAttemptNo()) > 0) {
                throw new IllegalStateException("DLQ evidence names an unknown physical attempt");
            }
        }
    }

    private SystemMutationResult persistSystemResult(
            final SystemMutation mutation,
            final SourcePosition position,
            final ApplyStatus status,
            final StableCode code) {
        final SystemMutationResult result =
                SystemMutationResult.from(mutation, status, code, position.canonicalBytes());
        store.write(batch -> {
            writeSystemResult(batch, result);
            writePosition(batch, position);
        });
        lastAppliedSourcePosition = position;
        mutationSequence = nextMutationSequence();
        return result;
    }

    /**
     * Persists the source-ordered retirement intent and its immutable protection
     * set. External deletion, Floor release and delete confirmation remain
     * separate mutations and are never inferred here.
     */
    private SystemMutationResult applyResourceRetireIntentMutation(
            final SystemMutation mutation, final SourcePosition sourcePosition) {
        final ResourceRetireIntentBody body = ResourceRetireIntentBody.decode(mutation.canonicalBody());
        body.validateProtectionSourceShard(store.shardId());
        final byte[] expectedLogicalIdentity = SystemMutation.computeResourceRetireLogicalIdentity(
                body.resourceKind(), body.resource().identityHash(), body.expectedResourceStateVersion());
        if (!Bytes.constantTimeEquals(mutation.logicalOperationIdentity(), expectedLogicalIdentity)) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final ResourceRetireIntentRecord prior = getResourceRetireIntent(
                body.resourceKind(), body.resource().identityHash(), body.expectedResourceStateVersion());
        if (prior != null) {
            if (Bytes.constantTimeEquals(prior.mutationId(), mutation.systemMutationId())
                    && Bytes.constantTimeEquals(prior.mutationHash(), mutation.mutationHash())) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.OK);
            }
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.VERSION_CONFLICT);
        }
        final ResourceRetireIntentRecord record = new ResourceRetireIntentRecord(
                mutation.systemMutationId(),
                mutation.mutationHash(),
                body.resourceKind(),
                body.resource().canonicalBytes(),
                body.resource().identityHash(),
                body.expectedResourceStateVersion(),
                nextMutationSequence(),
                body.protections().canonicalBytes(),
                sourcePosition.canonicalBytes());
        final SystemMutationResult result = SystemMutationResult.from(
                mutation, ApplyStatus.APPLIED, StableCode.OK, sourcePosition.canonicalBytes());
        store.write(batch -> {
            batch.putValue(
                    ColumnFamily.GC,
                    ResourceRetireIntentRecord.VALUE_TYPE,
                    KeyCodec.gcRetireIntent(
                            record.resourceKind(),
                            record.resourceIdentityHash(),
                            record.expectedResourceStateVersion()),
                    record.encode());
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        return result;
    }

    /**
     * Records an exact delete response against an already applied retire intent.
     * Provider deletion, Recovery Floor advancement and quota release remain outside this local projection.
     */
    private SystemMutationResult applyResourceDeleteConfirmedMutation(
            final SystemMutation mutation, final SourcePosition sourcePosition) {
        final ResourceDeleteConfirmedBody body = ResourceDeleteConfirmedBody.decode(mutation.canonicalBody());
        if (!Bytes.constantTimeEquals(
                mutation.logicalOperationIdentity(), body.intent().mutationId())) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final RetireIntentLookup lookup = findRetireIntent(body.intent());
        if (lookup == null) {
            return persistSystemResult(
                    mutation, sourcePosition, ApplyStatus.REJECTED, StableCode.STALE_SYSTEM_MUTATION);
        }
        ResourceRetireIntentBody.validateExternalDeleteIdentity(
                lookup.resourceKind(),
                lookup.intent().resourceIdentity(),
                body.evidence().observedImmutableVersion(),
                body.evidence().observedEtag(),
                body.outcome());
        final ResourceDeleteConfirmedRecord prior = getResourceDeleteConfirmation(
                lookup.resourceKind(),
                body.intent().resourceIdentityHash(),
                body.intent().expectedResourceStateVersion());
        if (prior != null) {
            if (Bytes.constantTimeEquals(prior.confirmationMutationId(), mutation.systemMutationId())
                    && Bytes.constantTimeEquals(prior.confirmationMutationHash(), mutation.mutationHash())) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.OK);
            }
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.VERSION_CONFLICT);
        }
        final ResourceDeleteConfirmedRecord record = new ResourceDeleteConfirmedRecord(
                mutation.systemMutationId(),
                mutation.mutationHash(),
                lookup.intent(),
                body.outcome(),
                nextMutationSequence(),
                body.evidence().providerRequestIdHash(),
                body.evidence().observedImmutableVersion(),
                body.evidence().observedEtag(),
                body.evidence().responseHash(),
                body.evidence().observedAt().canonicalBytes(),
                body.confirmedAt().canonicalBytes(),
                sourcePosition.canonicalBytes());
        final SystemMutationResult result = SystemMutationResult.from(
                mutation, ApplyStatus.APPLIED, StableCode.OK, sourcePosition.canonicalBytes());
        store.write(batch -> {
            batch.putValue(
                    ColumnFamily.GC,
                    ResourceDeleteConfirmedRecord.VALUE_TYPE,
                    KeyCodec.gcRetireIntent(
                            lookup.resourceKind(),
                            record.retireIntent().resourceIdentityHash(),
                            record.retireIntent().expectedResourceStateVersion()),
                    record.encode());
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        return result;
    }

    private RetireIntentLookup findRetireIntent(final ResourceDeleteConfirmedBody.RetireIntentRef reference) {
        for (ResourceKind resourceKind : ResourceKind.values()) {
            final ResourceRetireIntentRecord intent = getResourceRetireIntent(
                    resourceKind, reference.resourceIdentityHash(), reference.expectedResourceStateVersion());
            if (intent != null
                    && Bytes.constantTimeEquals(intent.mutationId(), reference.mutationId())
                    && Bytes.constantTimeEquals(intent.mutationHash(), reference.mutationHash())) {
                return new RetireIntentLookup(resourceKind, intent);
            }
        }
        return null;
    }

    private byte[] gcValue(
            final ResourceKind resourceKind, final byte[] resourceIdentityHash, final long expectedVersion) {
        return store.get(ColumnFamily.GC, KeyCodec.gcRetireIntent(resourceKind, resourceIdentityHash, expectedVersion));
    }

    private ResourceRetireIntentRecord validateGcIntentIdentity(
            final ResourceRetireIntentRecord intent,
            final ResourceKind resourceKind,
            final byte[] resourceIdentityHash,
            final long expectedVersion) {
        if (intent.resourceKind() != resourceKind
                || !Bytes.constantTimeEquals(intent.resourceIdentityHash(), resourceIdentityHash)
                || intent.expectedResourceStateVersion() != expectedVersion) {
            throw new IllegalStateException("GC retire intent key/value identity mismatch");
        }
        validateSourcePositionShard(intent.appliedSourcePosition(), "GC retire intent lookup");
        return intent;
    }

    private ResourceDeleteConfirmedRecord validateGcConfirmationIdentity(
            final ResourceDeleteConfirmedRecord confirmation,
            final ResourceKind resourceKind,
            final byte[] resourceIdentityHash,
            final long expectedVersion) {
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

    private boolean systemMutationRetryWindowExpired(
            final SystemMutation mutation, final SourcePosition sourcePosition) {
        return sourcePosition.brokerPersistenceTimeEpochMs() > mutation.retryUntilEpochMs()
                || (closedIngressDeadlineThrough >= 0 && mutation.retryUntilEpochMs() <= closedIngressDeadlineThrough);
    }

    private SystemMutationResult persistExpiredSystemPositionOnly(
            final SystemMutation mutation, final SourcePosition sourcePosition) {
        final SystemMutationResult result = expiredSystemMutationResult(mutation, sourcePosition);
        store.write(batch -> {
            batch.putValue(
                    ColumnFamily.DEDUPE,
                    DEDUPE_POSITION_VALUE_TYPE,
                    KeyCodec.dedupePosition(sourcePosition.canonicalBytes()),
                    mutation.systemMutationId());
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        return result;
    }

    private static SystemMutationResult expiredSystemMutationResult(
            final SystemMutation mutation, final SourcePosition sourcePosition) {
        return SystemMutationResult.from(
                mutation,
                ApplyStatus.REJECTED,
                StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED,
                sourcePosition.canonicalBytes());
    }

    private record RetireIntentLookup(ResourceKind resourceKind, ResourceRetireIntentRecord intent) {}

    private void writeSystemResult(final ShardStore.Batch batch, final SystemMutationResult result)
            throws org.rocksdb.RocksDBException {
        batch.putValue(
                ColumnFamily.DEDUPE,
                SystemMutationResult.VALUE_TYPE,
                KeyCodec.dedupeSystemMutation(result.mutationId()),
                result.encode());
        // POSITION is a closed physical-record audit for both source-log
        // branches. Client Commands keep their commandId[41] payload; a
        // System Mutation uses its mutationId[32]. The audit is what lets an
        // exact replay at the already-advanced current position be
        // distinguished from a caller pairing a different mutation with
        // that position.
        batch.putValue(
                ColumnFamily.DEDUPE,
                DEDUPE_POSITION_VALUE_TYPE,
                KeyCodec.dedupePosition(result.appliedSourcePosition()),
                result.mutationId());
    }

    private void validateMutationShard(final SystemMutation mutation, final SourcePosition sourcePosition) {
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        if (!store.shardId().equals(mutation.shardId()) || !store.shardId().equals(sourcePosition.shardId())) {
            throw new IllegalArgumentException("System Mutation/source position does not belong to shard");
        }
    }

    private static com.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field field(
            final List<com.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field> fields, final int number) {
        for (int index = 3; index < fields.size(); index++) {
            if (fields.get(index).number() == number) {
                return fields.get(index);
            }
        }
        throw new IllegalArgumentException("missing System Mutation operation field " + number);
    }

    private static long bodyNonNegative(
            final com.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.wireType() != 0 || field.number() != number || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("invalid System Mutation scalar field " + number);
        }
        return field.unsignedValue();
    }

    private static int bodyInt(
            final com.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = bodyNonNegative(field, number);
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("System Mutation field exceeds Java int range: " + number);
        }
        return (int) value;
    }

    private static int bodyUint32Bits(
            final com.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = bodyNonNegative(field, number);
        if (value > 0xffff_ffffL) {
            throw new IllegalArgumentException("System Mutation field exceeds uint32 range: " + number);
        }
        return (int) value;
    }

    private static byte[] bytesBody(
            final com.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.wireType() != 2 || field.number() != number) {
            throw new IllegalArgumentException("invalid System Mutation bytes field " + number);
        }
        return field.rawValue();
    }

    private static byte[] fixedBodyBytes(
            final com.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field field,
            final int number,
            final int length) {
        final byte[] value = bytesBody(field, number);
        Bytes.requireLength(value, length, "System Mutation field " + number);
        return value;
    }

    private static byte[] optionalBodyBytes(
            final List<com.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field> fields, final int number) {
        for (int index = 3; index < fields.size(); index++) {
            if (fields.get(index).number() == number) {
                return bytesBody(fields.get(index), number);
            }
        }
        return new byte[0];
    }

    public synchronized TerminalGenerationRecord getTerminalGeneration(
            final DelayMessageId messageId, final int generation) {
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
    public synchronized PublishAttemptLedger getPublishAttempt(final byte[] publishAttemptId, final long ownerEpoch) {
        Bytes.requireLength(publishAttemptId, PublishAttemptLedger.HASH_LENGTH, "publishAttemptId");
        if (ownerEpoch == 0) {
            throw new IllegalArgumentException("ownerEpoch must be positive");
        }
        final PublishAttemptLedger publishing =
                readPublishAttempt(publishAttemptId, ownerEpoch, INFLIGHT_PUBLISHING_KIND);
        final PublishAttemptLedger uncertain =
                readPublishAttempt(publishAttemptId, ownerEpoch, INFLIGHT_UNCERTAIN_KIND);
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
        final long configuredInflightLimit = configuredInflightLedgerLimit();
        final int limit = boundedLimitPlusOne(configuredInflightLimit);
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> entries =
                store.scan(ColumnFamily.INFLIGHT, new byte[] {INFLIGHT_PUBLISHING_KIND, 1}, new byte[] {4, 1}, limit);
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
        if (entries.size() >= limit && configuredInflightLimit < Integer.MAX_VALUE) {
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
        final long configuredInflightLimit = configuredInflightLedgerLimit();
        final int limit = boundedLimitPlusOne(configuredInflightLimit);
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> entries =
                store.scan(ColumnFamily.INFLIGHT, new byte[] {INFLIGHT_PUBLISHING_KIND, 1}, new byte[] {4, 1}, limit);
        if (entries.size() >= limit && configuredInflightLimit < Integer.MAX_VALUE) {
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
     * Persists the adapter-owned Attempt Journal mapping evidence for one
     * already durable PUBLISHING attempt. This local evidence update does not
     * advance the Command/Shard source position; callers must invoke it from
     * the fenced single-writer adapter event loop only after the exact Journal
     * append has returned a durable position.
     */
    synchronized PublishAttemptLedger recordAttemptJournalMapping(
            final byte[] publishAttemptId, final long ownerEpoch, final long sequenceId, final byte[] journalPosition) {
        final PublishAttemptLedger current = requirePublishingAttempt(publishAttemptId, ownerEpoch);
        final PublishAttemptLedger next = current.withDurableJournalMapping(sequenceId, journalPosition);
        persistAttemptJournalProjection(current, next);
        return next;
    }

    /** Holds a mapped PUBLISHING attempt while a RETIRED_NOT_PUBLISHED append is in flight. */
    synchronized PublishAttemptLedger markAttemptJournalRetirementPending(
            final byte[] publishAttemptId, final long ownerEpoch) {
        final PublishAttemptLedger current = requirePublishingAttempt(publishAttemptId, ownerEpoch);
        final PublishAttemptLedger next = current.withRetirementPending();
        persistAttemptJournalProjection(current, next);
        return next;
    }

    /** Persists the acknowledged retirement position before source-ordered retry/terminalization. */
    synchronized PublishAttemptLedger recordAttemptJournalRetirement(
            final byte[] publishAttemptId, final long ownerEpoch, final byte[] journalPosition) {
        final PublishAttemptLedger current = requirePublishingAttempt(publishAttemptId, ownerEpoch);
        final PublishAttemptLedger next = current.withDurableRetirement(journalPosition);
        persistAttemptJournalProjection(current, next);
        return next;
    }

    /**
     * Narrow bridge used by the fenced {@code OwnedDelayShard} adapter loop.
     * The caller supplies the persisted ledger Owner Epoch; this method never
     * changes ownership and only applies one of the three closed Journal
     * projections after its physical append has been acknowledged.
     */
    public synchronized PublishAttemptLedger applyOwnedAttemptJournalProjection(
            final AttemptJournalProjection operation,
            final byte[] publishAttemptId,
            final long admittedOwnerEpoch,
            final long sequenceId,
            final byte[] journalPosition) {
        Objects.requireNonNull(operation, "operation");
        return switch (operation) {
            case MAPPED ->
                recordAttemptJournalMapping(
                        publishAttemptId,
                        admittedOwnerEpoch,
                        sequenceId,
                        Objects.requireNonNull(journalPosition, "journalPosition"));
            case RETIREMENT_PENDING -> {
                if (journalPosition != null && journalPosition.length != 0) {
                    throw new IllegalArgumentException("retirement-pending projection cannot carry a position");
                }
                yield markAttemptJournalRetirementPending(publishAttemptId, admittedOwnerEpoch);
            }
            case RETIRED ->
                recordAttemptJournalRetirement(
                        publishAttemptId,
                        admittedOwnerEpoch,
                        Objects.requireNonNull(journalPosition, "journalPosition"));
        };
    }

    /** Closed adapter-owned Journal projection operations. */
    public enum AttemptJournalProjection {
        MAPPED,
        RETIREMENT_PENDING,
        RETIRED
    }

    private PublishAttemptLedger requirePublishingAttempt(final byte[] publishAttemptId, final long ownerEpoch) {
        final PublishAttemptLedger ledger = getPublishAttempt(publishAttemptId, ownerEpoch);
        if (ledger == null || ledger.state() != AttemptLedgerState.PUBLISHING) {
            throw new IllegalStateException("Attempt Journal update requires an open PUBLISHING ledger");
        }
        return ledger;
    }

    private void persistAttemptJournalProjection(final PublishAttemptLedger current, final PublishAttemptLedger next) {
        if (!Arrays.equals(current.encodedKey(), next.encodedKey())) {
            throw new IllegalStateException("Attempt Journal update changed the inflight key");
        }
        if (current.equals(next)) {
            return;
        }
        store.write(batch -> batch.putValue(
                ColumnFamily.INFLIGHT, PublishAttemptLedger.VALUE_TYPE, next.encodedKey(), next.encode()));
    }

    /**
     * Applies the durable part of Publish Admission. The complete signed Registry body is retained verbatim in the
     * ledger, while nested Claim/Certificate/Channel validation is deliberately owned by the pending admission
     * body codec. The message, timeline, READY projection, attempt key and source position commit in one batch.
     */
    synchronized PublishAttemptLedger admitPublishAttempt(
            final PublishAttemptLedger admission, final SourcePosition sourcePosition) {
        return admitPublishAttempt(admission, sourcePosition, null, false, false, OutcomeReserveUsage.empty(), null);
    }

    private PublishAttemptLedger admitPublishAttempt(
            final PublishAttemptLedger admission,
            final SourcePosition sourcePosition,
            final SystemMutationResult systemResult,
            final boolean claimMayBeMissing,
            final boolean uncertainRetryAdmission,
            final OutcomeReserveUsage admissionCharge,
            final SloSampleStart dueAdmissionStart) {
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
        if (current == null
                || (current.status() != MessageStatus.SCHEDULED && current.status() != MessageStatus.CLAIMED)
                || current.generation() != admission.generation()
                || !current.laneId().equals(admission.laneId())) {
            throw new IllegalStateException("publish admission is stale for the current message generation");
        }
        validateAdmissionBudget(
                admission.delayMessageId(), current, current.runtimeIndex(), uncertainRetryAdmission, sourcePosition);
        final ClaimRecord claim = current.status() == MessageStatus.CLAIMED
                ? getClaim(admission.claimId(), admission.ownerEpoch())
                : null;
        if (current.status() == MessageStatus.CLAIMED
                && ((!claimMayBeMissing && claim == null)
                        || (claim != null
                                && (!claim.delayMessageId().equals(admission.delayMessageId())
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
        validatePersistedRetryWindow(admission, current, lane, sourcePosition);
        final List<AttemptObligationRef> obligations =
                withObligation(current.runtimeIndex(), admission.obligationRef());
        MessageRecord next = MessageRecord.current(
                MessageStatus.PUBLISHING,
                current.generation(),
                Math.addExact(current.stateVersion(), 1),
                current.deliverAtEpochMs(),
                current.expireAtEpochMs(),
                current.laneId(),
                current.orderingMode(),
                current.payload(),
                current.scheduleSourcePosition(),
                current.payloadReference(),
                current.retryEligibilityAtEpochMs());
        next = next.withRuntimeIndex(GenerationRuntimeIndex.publishing(
                admission.publishAttemptId(),
                obligations,
                Math.addExact(current.runtimeIndex().admissionsUsed(), 1),
                Math.addExact(current.runtimeIndex().uncertainRetryAdmissionsUsed(), uncertainRetryAdmission ? 1 : 0),
                current.runtimeIndex().possibleDestinationDuplicate(),
                next.stateVersion()));
        final MessageRecord admissionNext = next;
        final byte[] priorTimelineKey =
                claim == null ? timelineKey(admission.delayMessageId(), current) : claim.timelineKey();
        final OutcomeReserveUsage nextOutcomeReserve = outcomeReserve.add(admissionCharge);
        final CapacityVector nextOutcomeReserveVector = outcomeReserveVector.add(outcomeCapacityCharge(admission));
        if (!nextOutcomeReserve.fits(
                OutcomeReserveUsage.empty(), config.maxOutcomeReserveRecords(), config.maxOutcomeReserveBytes())) {
            throw new IllegalStateException("Publish Admission outcome reserve exceeds shard grant");
        }
        validateOutcomeReserveVector(nextOutcomeReserveVector);
        LaneQuotaUsageProjection nextLaneQuota = laneQuotaUsage;
        final long quotaRevision = Math.max(1, quota.usageRevision());
        if (claim != null) {
            nextLaneQuota = mutateInflightQuotaUsage(
                    nextLaneQuota, claim.laneId(), claim.laneIncarnation(), claimCharge(claim), false, quotaRevision);
        }
        nextLaneQuota = mutateInflightQuotaUsage(
                nextLaneQuota,
                admission.laneId(),
                admission.laneIncarnation(),
                attemptCharge(admission),
                true,
                quotaRevision);
        final LaneQuotaUsageProjection projectedLaneQuota = nextLaneQuota;
        final Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections =
                readyProjections(sourcePosition, admission.delayMessageId(), current, next, null, projectedLaneQuota);
        store.write(batch -> {
            batch.delete(ColumnFamily.TIMELINE, priorTimelineKey);
            batch.delete(ColumnFamily.TIMELINE, expiryKey(admission.delayMessageId(), current));
            if (claim != null) {
                batch.delete(ColumnFamily.INFLIGHT, claim.encodedKey());
            }
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(admission.delayMessageId()), admissionNext.encode());
            batch.putValue(
                    ColumnFamily.INFLIGHT, PublishAttemptLedger.VALUE_TYPE, admission.encodedKey(), admission.encode());
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            if (systemResult != null) {
                writeSystemResult(batch, systemResult);
            }
            persistQuota(batch, quota, projectedLaneQuota);
            persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector, projectedLaneQuota);
            persistSloStart(batch, dueAdmissionStart);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        laneQuotaUsage = projectedLaneQuota;
        outcomeReserve = nextOutcomeReserve;
        outcomeReserveVector = nextOutcomeReserveVector;
        return admission;
    }

    /** Returns the outcome-reserve component of a canonical Admission ledger. */
    private OutcomeReserveUsage outcomeReserveCharge(final PublishAttemptLedger ledger) {
        try {
            return OutcomeReserveUsage.from(
                    PublishAdmissionBody.decode(ledger.admissionBytes()).chargeVector());
        } catch (RuntimeException legacyOrMalformedDirectLedger) {
            failClosedForMalformedCanonicalAdmission(ledger.admissionBytes(), legacyOrMalformedDirectLedger);
            // The public embedded admission helper predates ChargeVector and is
            // intentionally usable with synthetic test bytes. Such ledgers did
            // not consume this reserve and therefore release zero. A body with
            // the canonical System Mutation common-field prefix is never a
            // legacy adapter value: a malformed such body is an integrity
            // failure and must not be downgraded to zero charge.
            return OutcomeReserveUsage.empty();
        }
    }

    /**
     * A definitive outcome may release only the exact charge vector retained by
     * its Admission. Synthetic direct ledgers used by the embedded compatibility
     * seam carry no canonical Admission body and therefore retain the all-zero
     * vector; they must not accept an arbitrary transfer from the callback.
     */
    private boolean matchesRetainedOutcomeCharge(final PublishAttemptLedger ledger, final byte[] transfer) {
        final byte[] retained = retainedOutcomeCharge(ledger);
        return Bytes.constantTimeEquals(retained, transfer);
    }

    private byte[] retainedOutcomeCharge(final PublishAttemptLedger ledger) {
        try {
            return PublishAdmissionBody.decode(ledger.admissionBytes())
                    .chargeVector()
                    .canonicalBytes();
        } catch (RuntimeException legacyOrMalformedDirectLedger) {
            failClosedForMalformedCanonicalAdmission(ledger.admissionBytes(), legacyOrMalformedDirectLedger);
            return emptyChargeVectorCanonical();
        }
    }

    private static byte[] emptyChargeVectorCanonical() {
        return new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
                .canonicalBytes();
    }

    private OutcomeReserveUsage releasedOutcomeReserve(final PublishAttemptLedger ledger) {
        return outcomeReserve.remove(outcomeReserveCharge(ledger));
    }

    private CapacityVector outcomeCapacityCharge(final PublishAttemptLedger ledger) {
        try {
            return PublishAdmissionBody.decode(ledger.admissionBytes())
                    .chargeVector()
                    .toCapacityVector();
        } catch (RuntimeException legacyOrMalformedDirectLedger) {
            failClosedForMalformedCanonicalAdmission(ledger.admissionBytes(), legacyOrMalformedDirectLedger);
            // Synthetic direct ledgers do not carry a canonical ChargeVector.
            return CapacityVector.empty();
        }
    }

    /**
     * Keeps the embedded pre-registry ledger adapter bounded without allowing a
     * malformed canonical PUBLISH_ADMISSION body to masquerade as a zero-charge
     * legacy record. Canonical System Mutation bodies begin with field 1, a
     * length-delimited nested common-field block (protobuf tag {@code 0x0a});
     * arbitrary legacy fixture bytes do not claim that shape.
     */
    private static void failClosedForMalformedCanonicalAdmission(
            final byte[] admissionBytes, final RuntimeException failure) {
        if (admissionBytes != null && admissionBytes.length > 0 && (admissionBytes[0] & 0xff) == 0x0a) {
            throw new IllegalStateException("malformed canonical PUBLISH_ADMISSION body in attempt ledger", failure);
        }
    }

    private CapacityVector releasedOutcomeReserveVector(final PublishAttemptLedger ledger) {
        return outcomeReserveVector.subtract(outcomeCapacityCharge(ledger));
    }

    private void validateOutcomeReserveVector(final CapacityVector nextUsage) {
        if (capacityEnvelope != null
                && !capacityEnvelope.outcomeReserve().vector().covers(nextUsage)) {
            throw new IllegalStateException("Publish Admission outcome reserve exceeds immutable capacity grant");
        }
    }

    private void persistOutcomeReserve(
            final ShardStore.Batch batch,
            final OutcomeReserveUsage nextUsage,
            final CapacityVector nextVector,
            final LaneQuotaUsageProjection nextLaneQuota)
            throws org.rocksdb.RocksDBException {
        // Class 1 accepted only the pre-Registry ShardQuota projection. Once
        // a source-ordered mutation has written the canonical class-2 vector,
        // remove that legacy value so a later activation cannot validate a
        // stale scalar against the current ledgers.
        batch.delete(ColumnFamily.META, KeyCodec.metaQuota(META_LEGACY_QUOTA_USAGE));
        batch.putValue(
                ColumnFamily.META,
                7,
                KeyCodec.metaQuota(META_QUOTA_AGGREGATE_USAGE),
                aggregateQuotaUsage(nextLaneQuota, nextVector).canonicalBytes());
        if (capacityEnvelope != null && !nextVector.equals(outcomeReserveVector)) {
            final byte[] key = KeyCodec.metaControlReserve(
                    2, capacityEnvelope.outcomeReserve().grantId());
            if (nextVector.isZero()) {
                batch.delete(ColumnFamily.META, key);
            } else {
                batch.putValue(ColumnFamily.META, CAPACITY_RESERVE_VALUE_TYPE, key, nextVector.canonicalBytes());
            }
        }
    }

    /** Atomically records an unknown target result and moves the exact key to UNCERTAIN. */
    synchronized PublishAttemptLedger applyUnknownPublishOutcome(
            final byte[] publishAttemptId,
            final long ownerEpoch,
            final byte[] canonicalOutcome,
            final byte[] evidence,
            final SourcePosition sourcePosition) {
        return applyUnknownPublishOutcome(
                publishAttemptId, ownerEpoch, canonicalOutcome, evidence, sourcePosition, null, null);
    }

    private PublishAttemptLedger applyUnknownPublishOutcome(
            final byte[] publishAttemptId,
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
        if (current == null || compareGeneration(current.generation(), currentLedger.generation()) < 0) {
            throw new IllegalStateException("unknown outcome is stale for the current message");
        }
        if (compareGeneration(current.generation(), currentLedger.generation()) > 0) {
            return settleHistoricalUnknownObligation(
                    currentLedger, canonicalOutcome, evidence, sourcePosition, systemResult);
        }
        if (current.status() != MessageStatus.PUBLISHING) {
            throw new IllegalStateException("unknown outcome is stale for the current message");
        }
        final LaneRecord currentLane = readLane(current.laneId());
        if (!current.laneId().equals(currentLedger.laneId())) {
            throw new IllegalStateException("unknown outcome Lane identity does not match the attempt ledger");
        }
        if (currentLane == null) {
            throw new IllegalStateException("unknown outcome references a missing Lane");
        }
        if (currentLedger.hasRetryWindow()
                && !Arrays.equals(currentLane.laneIncarnation(), currentLedger.laneIncarnation())) {
            throw new IllegalStateException("unknown outcome Lane incarnation does not match the attempt ledger");
        }
        final boolean scheduleUncertainRetry = retryDecision != null
                && retryDecision.kind() == 2
                && (currentLane.admissionGate() != AdmissionGate.CLOSED
                        && currentLane.admissionGate() != AdmissionGate.RETIRED);
        final long retryAt;
        if (scheduleUncertainRetry) {
            final RetryPolicySemantic pinnedPolicy =
                    retryPolicyFor(currentLedger.delayMessageId(), current, sourcePosition);
            final int maxUncertainRetries =
                    pinnedPolicy == null ? config.maxUncertainRetries() : pinnedPolicy.maxUncertainRetries();
            final int maxPublishAdmissions =
                    pinnedPolicy == null ? config.maxPublishAdmissions() : pinnedPolicy.maxPublishAdmissions();
            if (current.orderingMode() != com.nereusstream.delay.protocol.OrderingMode.BEST_EFFORT
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
        final PublishAttemptLedger nextLedger =
                currentLedger.withUnknownOutcome(canonicalOutcome, evidence, sourcePosition.canonicalBytes());
        final List<AttemptObligationRef> nextObligations =
                withObligation(current.runtimeIndex(), nextLedger.obligationRef());
        MessageRecord next = MessageRecord.current(
                scheduleUncertainRetry ? MessageStatus.SCHEDULED : MessageStatus.UNCERTAIN,
                current.generation(),
                Math.addExact(current.stateVersion(), 1),
                current.deliverAtEpochMs(),
                current.expireAtEpochMs(),
                current.laneId(),
                current.orderingMode(),
                current.payload(),
                current.scheduleSourcePosition(),
                current.payloadReference(),
                retryAt);
        next = scheduleUncertainRetry
                ? next.withRuntimeIndex(timelineRuntimeIndex(
                        currentLedger.delayMessageId(),
                        next,
                        TimelineWorkKind.UNCERTAIN_RETRY,
                        Math.addExact(current.runtimeIndex().admissionsUsed(), 1),
                        next.stateVersion(),
                        UncertainRetryAuthority.PINNED_POLICY,
                        null,
                        null,
                        current.runtimeIndex(),
                        nextObligations))
                : next.withRuntimeIndex(GenerationRuntimeIndex.none(
                        GenerationAggregateState.UNCERTAIN,
                        nextObligations,
                        current.runtimeIndex().admissionsUsed(),
                        current.runtimeIndex().uncertainRetryAdmissionsUsed(),
                        current.runtimeIndex().possibleDestinationDuplicate(),
                        next.stateVersion()));
        final MessageRecord uncertainNext = next;
        final Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections =
                readyProjections(sourcePosition, currentLedger.delayMessageId(), current, next, null);
        store.write(batch -> {
            batch.delete(ColumnFamily.INFLIGHT, currentLedger.encodedKey());
            batch.putValue(
                    ColumnFamily.INFLIGHT,
                    PublishAttemptLedger.VALUE_TYPE,
                    nextLedger.encodedKey(),
                    nextLedger.encode());
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(nextLedger.delayMessageId()), uncertainNext.encode());
            if (scheduleUncertainRetry) {
                batch.putValue(
                        ColumnFamily.TIMELINE,
                        1,
                        timelineKey(nextLedger.delayMessageId(), uncertainNext),
                        encodeTimelineValue(nextLedger.delayMessageId(), uncertainNext));
                batch.putValue(
                        ColumnFamily.TIMELINE,
                        1,
                        expiryKey(nextLedger.delayMessageId(), uncertainNext),
                        encodeTimelineValue(nextLedger.delayMessageId(), uncertainNext));
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
        mutationSequence = nextMutationSequence();
        return nextLedger;
    }

    private PublishAttemptLedger settleHistoricalUnknownObligation(
            final PublishAttemptLedger ledger,
            final byte[] canonicalOutcome,
            final byte[] evidence,
            final SourcePosition sourcePosition,
            final SystemMutationResult systemResult) {
        final TerminalGenerationRecord summary = getTerminalGeneration(ledger.delayMessageId(), ledger.generation());
        if (summary == null || !summary.openObligations().contains(ledger.obligationRef())) {
            throw new IllegalStateException("historical terminal obligation summary is stale or missing");
        }
        final PublishAttemptLedger nextLedger =
                ledger.withUnknownOutcome(canonicalOutcome, evidence, sourcePosition.canonicalBytes());
        final List<AttemptObligationRef> obligations =
                withObligation(summary.openObligations(), nextLedger.obligationRef());
        final TerminalGenerationRecord nextSummary = new TerminalGenerationRecord(
                summary.messageId(),
                summary.generation(),
                summary.status(),
                summary.terminalCode(),
                summary.stateVersion(),
                summary.appliedSourcePosition(),
                summary.possibleDestinationDuplicate(),
                obligations);
        store.write(batch -> {
            batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
            batch.putValue(
                    ColumnFamily.INFLIGHT,
                    PublishAttemptLedger.VALUE_TYPE,
                    nextLedger.encodedKey(),
                    nextLedger.encode());
            batch.putValue(
                    ColumnFamily.TERMINAL,
                    1,
                    KeyCodec.terminalGeneration(ledger.delayMessageId(), ledger.generation()),
                    nextSummary.encode());
            if (systemResult != null) {
                writeSystemResult(batch, systemResult);
            }
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        return nextLedger;
    }

    /** Atomically closes a PUBLISHING attempt after a verified publish success. */
    synchronized MessageRecord applyPublishedPublishOutcome(
            final byte[] publishAttemptId, final long ownerEpoch, final SourcePosition sourcePosition) {
        return applyPublishedPublishOutcome(publishAttemptId, ownerEpoch, sourcePosition, null);
    }

    private MessageRecord applyPublishedPublishOutcome(
            final byte[] publishAttemptId,
            final long ownerEpoch,
            final SourcePosition sourcePosition,
            final SystemMutationResult systemResult) {
        validateMutationPosition(sourcePosition);
        final PublishAttemptLedger ledger = getPublishAttempt(publishAttemptId, ownerEpoch);
        if (ledger == null || ledger.state() != AttemptLedgerState.PUBLISHING) {
            throw new IllegalStateException("published outcome requires a PUBLISHING ledger");
        }
        return applyPublishedPublishOutcome(
                ledger, sourcePosition, systemResult, MessageStatus.PUBLISHING, MessageStatus.PUBLISHED);
    }

    private MessageRecord applyPublishedPublishOutcome(
            final PublishAttemptLedger ledger,
            final SourcePosition sourcePosition,
            final SystemMutationResult systemResult,
            final MessageStatus expectedMessageStatus,
            final MessageStatus terminalStatus) {
        if (terminalStatus != MessageStatus.PUBLISHED && terminalStatus != MessageStatus.HANDED_OFF) {
            throw new IllegalArgumentException("published outcome must select a success terminal status");
        }
        requirePublishAttemptLane(ledger, "published outcome");
        final MessageRecord current = getMessage(ledger.delayMessageId());
        if (current == null || compareGeneration(current.generation(), ledger.generation()) < 0) {
            throw new IllegalStateException("published outcome is stale for the current message");
        }
        if (compareGeneration(current.generation(), ledger.generation()) > 0) {
            return settleHistoricalTerminalObligation(ledger, sourcePosition, systemResult, true);
        }
        if (expectedMessageStatus == MessageStatus.UNCERTAIN
                && ledger.state() == AttemptLedgerState.UNCERTAIN
                && current.runtimeIndex().aggregateState() == GenerationAggregateState.UNCERTAIN
                && current.runtimeIndex().attemptObligations().contains(ledger.obligationRef())) {
            return settleVerifiedPublishedUncertainGeneration(
                    ledger, current, sourcePosition, systemResult, terminalStatus);
        }
        if (isTerminalStatus(current.status())) {
            return settleTerminalObligation(ledger, current, sourcePosition, systemResult, true);
        }
        if (current.status() != expectedMessageStatus) {
            throw new IllegalStateException("published outcome is stale for the current message");
        }
        MessageRecord next = MessageRecord.current(
                terminalStatus,
                current.generation(),
                Math.addExact(current.stateVersion(), 1),
                current.deliverAtEpochMs(),
                current.expireAtEpochMs(),
                current.laneId(),
                current.orderingMode(),
                current.payload(),
                current.scheduleSourcePosition(),
                current.payloadReference(),
                current.retryEligibilityAtEpochMs());
        next = next.withRuntimeIndex(GenerationRuntimeIndex.none(
                GenerationAggregateState.fromMessageStatus(terminalStatus),
                withoutObligation(current.runtimeIndex(), ledger.publishAttemptId()),
                current.runtimeIndex().admissionsUsed(),
                current.runtimeIndex().uncertainRetryAdmissionsUsed(),
                current.runtimeIndex().possibleDestinationDuplicate(),
                next.stateVersion()));
        final MessageRecord publishedNext = next;
        final TerminalGenerationRecord terminal = new TerminalGenerationRecord(
                ledger.delayMessageId(),
                ledger.generation(),
                terminalStatus,
                StableCode.OK,
                next.stateVersion(),
                sourcePosition.canonicalBytes(),
                next.runtimeIndex().possibleDestinationDuplicate(),
                next.runtimeIndex().attemptObligations());
        final OutcomeReserveUsage nextOutcomeReserve = releasedOutcomeReserve(ledger);
        final CapacityVector nextOutcomeReserveVector = releasedOutcomeReserveVector(ledger);
        final ShardQuota nextQuota = quota.removeSchedule(current.payloadLength());
        LaneQuotaUsageProjection nextLaneQuota = removeScheduleQuotaUsage(current, nextQuota);
        nextLaneQuota = mutateInflightQuotaUsage(
                nextLaneQuota,
                ledger.laneId(),
                ledger.laneIncarnation(),
                attemptCharge(ledger),
                false,
                Math.max(1, nextQuota.usageRevision()));
        final LaneQuotaUsageProjection projectedLaneQuota = nextLaneQuota;
        final Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections =
                readyProjections(sourcePosition, ledger.delayMessageId(), current, next, null, projectedLaneQuota);
        store.write(batch -> {
            batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(ledger.delayMessageId()), publishedNext.encode());
            batch.putValue(
                    ColumnFamily.TERMINAL,
                    1,
                    KeyCodec.terminalGeneration(ledger.delayMessageId(), ledger.generation()),
                    terminal.encode());
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            persistQuota(batch, nextQuota, projectedLaneQuota);
            persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector, projectedLaneQuota);
            if (systemResult != null) {
                writeSystemResult(batch, systemResult);
            }
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        quota = nextQuota;
        laneQuotaUsage = projectedLaneQuota;
        outcomeReserve = nextOutcomeReserve;
        outcomeReserveVector = nextOutcomeReserveVector;
        return next;
    }

    /**
     * Settles verified success while another reversible work item is still
     * present for the same generation. A late success proves that the
     * generation must not be retried: timeline/Claim work is removed and the
     * generation becomes terminal, while a different current PUBLISHING
     * attempt remains the sole current work and keeps the generation open.
     */
    private MessageRecord settleVerifiedPublishedUncertainGeneration(
            final PublishAttemptLedger ledger,
            final MessageRecord current,
            final SourcePosition sourcePosition,
            final SystemMutationResult systemResult,
            final MessageStatus terminalStatus) {
        if (terminalStatus != MessageStatus.PUBLISHED && terminalStatus != MessageStatus.HANDED_OFF) {
            throw new IllegalArgumentException("uncertain success must select a success terminal status");
        }
        final List<AttemptObligationRef> remaining =
                withoutObligation(current.runtimeIndex(), ledger.publishAttemptId());
        final boolean duplicate = true;
        final OutcomeReserveUsage nextOutcomeReserve = releasedOutcomeReserve(ledger);
        final CapacityVector nextOutcomeReserveVector = releasedOutcomeReserveVector(ledger);
        if (current.runtimeIndex().currentWorkKind() == CurrentSendWorkKind.PUBLISHING) {
            final byte[] currentAttemptId = current.runtimeIndex().publishAttemptId();
            if (currentAttemptId.length == 0 || Arrays.equals(currentAttemptId, ledger.publishAttemptId())) {
                throw new IllegalStateException("uncertain success cannot remove current publishing work");
            }
            final GenerationRuntimeIndex nextRuntime = GenerationRuntimeIndex.publishing(
                    currentAttemptId,
                    remaining,
                    current.runtimeIndex().admissionsUsed(),
                    current.runtimeIndex().uncertainRetryAdmissionsUsed(),
                    duplicate,
                    Math.addExact(current.runtimeIndex().runtimeRevision(), 1));
            final MessageRecord next = MessageRecord.current(
                            current.status(),
                            current.generation(),
                            current.stateVersion(),
                            current.deliverAtEpochMs(),
                            current.expireAtEpochMs(),
                            current.laneId(),
                            current.orderingMode(),
                            current.payload(),
                            current.scheduleSourcePosition(),
                            current.payloadReference(),
                            current.retryEligibilityAtEpochMs())
                    .withRuntimeIndex(nextRuntime);
            final LaneQuotaUsageProjection nextLaneQuota = removeAttemptQuotaUsage(ledger);
            store.write(batch -> {
                batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(ledger.delayMessageId()), next.encode());
                persistQuota(batch, quota, nextLaneQuota);
                persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector, nextLaneQuota);
                if (systemResult != null) {
                    writeSystemResult(batch, systemResult);
                }
                writePosition(batch, sourcePosition);
            });
            lastAppliedSourcePosition = sourcePosition;
            mutationSequence = nextMutationSequence();
            laneQuotaUsage = nextLaneQuota;
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
            if (claim == null
                    || !Arrays.equals(claim.claimId(), current.runtimeIndex().claimId())) {
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

        final MessageRecord next = MessageRecord.current(
                        terminalStatus,
                        current.generation(),
                        Math.addExact(current.stateVersion(), 1),
                        current.deliverAtEpochMs(),
                        current.expireAtEpochMs(),
                        current.laneId(),
                        current.orderingMode(),
                        current.payload(),
                        current.scheduleSourcePosition(),
                        current.payloadReference(),
                        current.retryEligibilityAtEpochMs())
                .withRuntimeIndex(GenerationRuntimeIndex.none(
                        GenerationAggregateState.fromMessageStatus(terminalStatus),
                        remaining,
                        current.runtimeIndex().admissionsUsed(),
                        current.runtimeIndex().uncertainRetryAdmissionsUsed(),
                        duplicate,
                        Math.addExact(current.stateVersion(), 1)));
        final TerminalGenerationRecord terminal = new TerminalGenerationRecord(
                ledger.delayMessageId(),
                ledger.generation(),
                terminalStatus,
                StableCode.OK,
                next.stateVersion(),
                sourcePosition.canonicalBytes(),
                duplicate,
                remaining);
        final ShardQuota nextQuota = quota.removeSchedule(current.payloadLength());
        LaneQuotaUsageProjection nextLaneQuota = removeScheduleQuotaUsage(current, nextQuota);
        nextLaneQuota = mutateInflightQuotaUsage(
                nextLaneQuota,
                ledger.laneId(),
                ledger.laneIncarnation(),
                attemptCharge(ledger),
                false,
                Math.max(1, nextQuota.usageRevision()));
        if (claimKey != null) {
            final ClaimRecord claim = findClaimForMessage(ledger.delayMessageId());
            if (claim == null) {
                throw new IllegalStateException("uncertain Claim work is missing its exact record");
            }
            nextLaneQuota = mutateInflightQuotaUsage(
                    nextLaneQuota,
                    claim.laneId(),
                    claim.laneIncarnation(),
                    claimCharge(claim),
                    false,
                    Math.max(1, nextQuota.usageRevision()));
        }
        final LaneQuotaUsageProjection projectedLaneQuota = nextLaneQuota;
        final Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections =
                readyProjections(sourcePosition, ledger.delayMessageId(), current, next, null, projectedLaneQuota);
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
            batch.putValue(
                    ColumnFamily.TERMINAL,
                    1,
                    KeyCodec.terminalGeneration(ledger.delayMessageId(), ledger.generation()),
                    terminal.encode());
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            persistQuota(batch, nextQuota, projectedLaneQuota);
            persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector, projectedLaneQuota);
            if (systemResult != null) {
                writeSystemResult(batch, systemResult);
            }
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        quota = nextQuota;
        laneQuotaUsage = projectedLaneQuota;
        outcomeReserve = nextOutcomeReserve;
        outcomeReserveVector = nextOutcomeReserveVector;
        return next;
    }

    private MessageRecord settleTerminalObligation(
            final PublishAttemptLedger ledger,
            final MessageRecord current,
            final SourcePosition sourcePosition,
            final SystemMutationResult systemResult,
            final boolean verifiedPublished) {
        final TerminalGenerationRecord summary = getTerminalGeneration(ledger.delayMessageId(), ledger.generation());
        if (summary == null
                || !containsObligation(current.runtimeIndex(), ledger.obligationRef())
                || !summary.openObligations().equals(current.runtimeIndex().attemptObligations())) {
            throw new IllegalStateException("terminal obligation summary is stale or missing");
        }
        final List<AttemptObligationRef> remaining =
                withoutObligation(current.runtimeIndex(), ledger.publishAttemptId());
        final boolean duplicate = current.runtimeIndex().possibleDestinationDuplicate() || verifiedPublished;
        final MessageRecord next = MessageRecord.current(
                        current.status(),
                        current.generation(),
                        current.stateVersion(),
                        current.deliverAtEpochMs(),
                        current.expireAtEpochMs(),
                        current.laneId(),
                        current.orderingMode(),
                        current.payload(),
                        current.scheduleSourcePosition(),
                        current.payloadReference(),
                        current.retryEligibilityAtEpochMs())
                .withRuntimeIndex(GenerationRuntimeIndex.none(
                        GenerationAggregateState.fromMessageStatus(current.status()),
                        remaining,
                        current.runtimeIndex().admissionsUsed(),
                        current.runtimeIndex().uncertainRetryAdmissionsUsed(),
                        duplicate,
                        Math.addExact(current.runtimeIndex().runtimeRevision(), 1)));
        final TerminalGenerationRecord nextSummary = new TerminalGenerationRecord(
                summary.messageId(),
                summary.generation(),
                summary.status(),
                summary.terminalCode(),
                summary.stateVersion(),
                summary.appliedSourcePosition(),
                duplicate,
                remaining);
        final OutcomeReserveUsage nextOutcomeReserve = releasedOutcomeReserve(ledger);
        final CapacityVector nextOutcomeReserveVector = releasedOutcomeReserveVector(ledger);
        final LaneQuotaUsageProjection nextLaneQuota = removeAttemptQuotaUsage(ledger);
        store.write(batch -> {
            batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(ledger.delayMessageId()), next.encode());
            batch.putValue(
                    ColumnFamily.TERMINAL,
                    1,
                    KeyCodec.terminalGeneration(ledger.delayMessageId(), ledger.generation()),
                    nextSummary.encode());
            persistQuota(batch, quota, nextLaneQuota);
            persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector, nextLaneQuota);
            if (systemResult != null) {
                writeSystemResult(batch, systemResult);
            }
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        laneQuotaUsage = nextLaneQuota;
        outcomeReserve = nextOutcomeReserve;
        outcomeReserveVector = nextOutcomeReserveVector;
        return next;
    }

    /** Settles an old-generation obligation using its terminal summary only. */
    private MessageRecord settleHistoricalTerminalObligation(
            final PublishAttemptLedger ledger,
            final SourcePosition sourcePosition,
            final SystemMutationResult systemResult,
            final boolean verifiedPublished) {
        final TerminalGenerationRecord summary = getTerminalGeneration(ledger.delayMessageId(), ledger.generation());
        if (summary == null || !summary.openObligations().contains(ledger.obligationRef())) {
            throw new IllegalStateException("historical terminal obligation summary is stale or missing");
        }
        final List<AttemptObligationRef> remaining =
                withoutObligation(summary.openObligations(), ledger.publishAttemptId());
        final boolean duplicate = summary.possibleDestinationDuplicate() || verifiedPublished;
        final TerminalGenerationRecord nextSummary = new TerminalGenerationRecord(
                summary.messageId(),
                summary.generation(),
                summary.status(),
                summary.terminalCode(),
                summary.stateVersion(),
                summary.appliedSourcePosition(),
                duplicate,
                remaining);
        final OutcomeReserveUsage nextOutcomeReserve = releasedOutcomeReserve(ledger);
        final CapacityVector nextOutcomeReserveVector = releasedOutcomeReserveVector(ledger);
        final MessageRecord current = getMessage(ledger.delayMessageId());
        final LaneQuotaUsageProjection nextLaneQuota = removeAttemptQuotaUsage(ledger);
        store.write(batch -> {
            batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
            batch.putValue(
                    ColumnFamily.TERMINAL,
                    1,
                    KeyCodec.terminalGeneration(ledger.delayMessageId(), ledger.generation()),
                    nextSummary.encode());
            persistQuota(batch, quota, nextLaneQuota);
            persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector, nextLaneQuota);
            if (systemResult != null) {
                writeSystemResult(batch, systemResult);
            }
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence = nextMutationSequence();
        laneQuotaUsage = nextLaneQuota;
        outcomeReserve = nextOutcomeReserve;
        outcomeReserveVector = nextOutcomeReserveVector;
        return current;
    }

    public synchronized LaneRecord getLane(final com.nereusstream.delay.protocol.DestinationLaneId laneId) {
        return readLane(laneId);
    }

    /**
     * Returns the typed immutable Lane projection, or {@code null} for a
     * legacy compatibility Lane. Production activation requires this typed
     * state so Profile/capability/tuple identity cannot be replaced by a
     * readiness-only compatibility record.
     */
    public synchronized ActiveLaneState getActiveLaneState(
            final com.nereusstream.delay.protocol.DestinationLaneId laneId) {
        final LaneValue value = readLaneValue(Objects.requireNonNull(laneId, "laneId"));
        return value == null ? null : value.typedActiveState();
    }

    /** Returns the durable local close cursor, if this Lane has not finished materialization. */
    public synchronized LaneCloseMaterializationCursor getLaneCloseCursor(
            final com.nereusstream.delay.protocol.DestinationLaneId laneId) {
        Objects.requireNonNull(laneId, "laneId");
        final LaneRecord lane = readLane(laneId);
        if (lane == null || lane.admissionGate() != AdmissionGate.CLOSED) {
            return null;
        }
        final var value = store.getValue(
                ColumnFamily.TIMELINE,
                closeCursorKey(laneId, lane.laneControlVersion()),
                LaneCloseMaterializationCursor.VALUE_TYPE);
        if (value == null) {
            return null;
        }
        final LaneCloseMaterializationCursor cursor = LaneCloseMaterializationCursor.decode(value.payload());
        if (!cursor.laneId().equals(laneId)
                || cursor.closeVersion() != lane.laneControlVersion()
                || !Arrays.equals(cursor.laneIncarnation(), lane.laneIncarnation())
                || !store.shardId()
                        .equals(SourcePositionCodec.decode(cursor.closeSourcePosition())
                                .shardId())) {
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
    synchronized List<LaneCloseMaterializationWork> discoverLaneCloseMaterialization(final int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        final int scanLimit = limit == Integer.MAX_VALUE ? Integer.MAX_VALUE : Math.addExact(limit, 1);
        final List<LaneCloseMaterializationWork> result = discoverLaneCloseMaterialization(
                new SchedulerBudget(scanLimit, Long.MAX_VALUE, Long.MAX_VALUE), () -> 0);
        if (result.size() > limit) {
            throw new IllegalStateException("Lane close materialization work exceeds scheduler bound");
        }
        return result;
    }

    /** Returns validated close cursors under record, actual-byte and elapsed scan bounds. */
    public synchronized List<LaneCloseMaterializationWork> discoverLaneCloseMaterialization(
            final SchedulerBudget budget, final LongSupplier monotonicClockNanos) {
        final SchedulerBudget bounded = Objects.requireNonNull(budget, "Lane close discovery budget");
        final LongSupplier clock = Objects.requireNonNull(monotonicClockNanos, "Lane close discovery monotonic clock");
        final List<LaneCloseMaterializationWork> result = new ArrayList<>();
        final BoundedReadBudget readBudget =
                new BoundedReadBudget(bounded.maxBytes(), bounded.maxElapsedNanos(), clock);
        final long[] completedCharge = {0};
        store.visit(
                ColumnFamily.TIMELINE,
                new byte[] {6, 1, LaneCloseMaterializationCursor.SYSTEM_WORK_KIND},
                new byte[] {6, 1, (byte) (LaneCloseMaterializationCursor.SYSTEM_WORK_KIND + 1)},
                bounded.maxMessages(),
                readBudget,
                (entry, sharedBudget) -> {
                    final long indexCharge = sharedBudget.chargedBytes() - completedCharge[0];
                    final SystemTimelineKey key = decodeLaneCloseWorkKey(entry.key());
                    final LaneCloseMaterializationCursor cursor = LaneCloseMaterializationCursor.decode(
                            ValueEnvelope.decode(entry.value(), LaneCloseMaterializationCursor.VALUE_TYPE)
                                    .payload());
                    final DestinationLaneId laneId = new DestinationLaneId(key.workId());
                    if (!cursor.laneId().equals(laneId)
                            || cursor.closeVersion() != key.workVersion()
                            || key.nextEligibleAtEpochMs() != 0) {
                        throw new IllegalStateException("Lane close system work key/value identity mismatch");
                    }
                    validateSourcePositionShard(cursor.closeSourcePosition(), "Lane close materialization discovery");
                    if (!sharedBudget.beforeRead()) {
                        return false;
                    }
                    final byte[] laneKey = KeyCodec.metaLane(laneId);
                    final byte[] rawLane = store.get(ColumnFamily.META, laneKey);
                    final int laneValueBytes = rawLane == null ? 0 : rawLane.length;
                    if (!sharedBudget.tryCharge(laneKey.length, laneValueBytes)) {
                        final long candidateBytes =
                                Math.addExact(indexCharge, Math.addExact((long) laneKey.length, laneValueBytes));
                        if (candidateBytes > sharedBudget.maxBytes()) {
                            throw new IllegalStateException("Lane close discovery candidate exceeds byte budget");
                        }
                        return false;
                    }
                    final LaneValue laneValue = rawLane == null
                            ? null
                            : validateLaneValueIdentity(
                                    laneId,
                                    decodeLaneValue(
                                            ValueEnvelope.decode(rawLane, 2).payload()));
                    final LaneRecord lane = laneValue == null
                            ? null
                            : laneValue.isActive()
                                    ? laneValue.asLaneRecord()
                                    : new LaneRecord(
                                            laneValue.terminalGuard().laneId(),
                                            laneValue.terminalGuard().laneIncarnation(),
                                            laneValue.terminalGuard().laneControlVersion(),
                                            0,
                                            AdmissionGate.RETIRED,
                                            RuntimeReadiness.BLOCKED,
                                            1,
                                            0);
                    if (lane == null
                            || lane.admissionGate() != AdmissionGate.CLOSED
                            || lane.laneControlVersion() != cursor.closeVersion()
                            || !Arrays.equals(lane.laneIncarnation(), cursor.laneIncarnation())) {
                        throw new IllegalStateException("Lane close system work points to a non-closed Lane");
                    }
                    result.add(new LaneCloseMaterializationWork(
                            laneId, cursor.closeVersion(), key.nextEligibleAtEpochMs(), cursor));
                    completedCharge[0] = sharedBudget.chargedBytes();
                    return true;
                });
        return List.copyOf(result);
    }

    /**
     * Applies one bounded, quota-neutral close-materialization batch. The
     * source-ordered marker is the semantic boundary; this method only resumes
     * the persisted cursor and never reclassifies an admitted obligation.
     */
    synchronized LaneCloseMaterializationResult materializeClosedLane(
            final com.nereusstream.delay.protocol.DestinationLaneId laneId, final int maxRecords) {
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
        if (lane == null
                || lane.admissionGate() != AdmissionGate.CLOSED
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
                ? scanAfter(ColumnFamily.ID, new byte[] {1, 1}, new byte[] {2, 1}, cursor.lastKey(), bound)
                : scanAfter(ColumnFamily.ID, new byte[] {2, 1}, new byte[] {3, 1}, cursor.lastKey(), bound);
        final List<ClosedMessageAction> messageActions = cursor.phase() == LaneCloseMaterializationCursor.Phase.MESSAGES
                ? prepareClosedMessageActions(cursor, scan.entries(), closePosition)
                : List.of();
        final List<ClosedReservationAction> reservationActions =
                cursor.phase() == LaneCloseMaterializationCursor.Phase.RESERVATIONS
                        ? prepareClosedReservationActions(cursor, scan.entries(), closePosition)
                        : List.of();
        final LaneCloseMaterializationCursor nextCursor;
        if (scan.hasMore()) {
            nextCursor =
                    cursor.advance(scan.entries().get(scan.entries().size() - 1).key());
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
                batch.putValue(
                        ColumnFamily.ID,
                        1,
                        KeyCodec.idMessage(action.messageId()),
                        action.terminalMessage().encode());
                batch.putValue(
                        ColumnFamily.TERMINAL,
                        1,
                        KeyCodec.terminalGeneration(
                                action.messageId(), action.terminalMessage().generation()),
                        action.terminal().encode());
                final DlqExportRecord dlqExport = DlqExportRecord.notConfigured(
                        action.messageId(),
                        action.terminalMessage().generation(),
                        action.terminalMessage().stateVersion(),
                        action.terminal().appliedSourcePosition());
                batch.putValue(
                        ColumnFamily.TERMINAL,
                        DlqExportRecord.VALUE_TYPE,
                        KeyCodec.terminalDlqExport(dlqExport.dlqExportId()),
                        dlqExport.encode());
            }
            for (ClosedReservationAction action : reservationActions) {
                batch.putValue(
                        ColumnFamily.ID,
                        2,
                        KeyCodec.idReservation(action.reservation().reservationId()),
                        action.closedReservation().encode());
                batch.delete(
                        ColumnFamily.TIMELINE,
                        KeyCodec.reservationExpiry(
                                action.reservation().reservationExpiryEpochMs(),
                                action.reservation().reservationId()));
            }
            if (nextCursor == null) {
                batch.delete(ColumnFamily.TIMELINE, closeCursorKey(cursor));
            } else {
                batch.putValue(
                        ColumnFamily.TIMELINE,
                        LaneCloseMaterializationCursor.VALUE_TYPE,
                        closeCursorKey(nextCursor),
                        nextCursor.canonicalBytes());
                if (!Arrays.equals(closeCursorKey(cursor), closeCursorKey(nextCursor))) {
                    batch.delete(ColumnFamily.TIMELINE, closeCursorKey(cursor));
                }
            }
        });
        return new LaneCloseMaterializationResult(
                laneId,
                cursor.closeVersion(),
                scan.entries().size(),
                messageActions.size(),
                reservationActions.size(),
                nextCursor == null);
    }

    /**
     * Materializes one exact close-cursor candidate after a bounded GC queue
     * wait. A different local turn may have advanced or removed the cursor
     * while this candidate was queued; that is a no-op outcome, not permission
     * to apply the candidate to a newer cursor without reporting the drift.
     */
    public synchronized LaneCloseMaterializationExecutionResult materializeClosedLane(
            final LaneCloseMaterializationWork candidate, final int maxRecords) {
        final LaneCloseMaterializationWork expected =
                Objects.requireNonNull(candidate, "Lane close materialization candidate");
        if (maxRecords <= 0) {
            throw new IllegalArgumentException("maxRecords must be positive");
        }
        final LaneCloseMaterializationCursor current = getLaneCloseCursor(expected.laneId());
        if (current == null) {
            return LaneCloseMaterializationExecutionResult.notFound();
        }
        if (!Arrays.equals(current.canonicalBytes(), expected.cursor().canonicalBytes())) {
            return LaneCloseMaterializationExecutionResult.stale(current);
        }
        return LaneCloseMaterializationExecutionResult.materialized(
                materializeClosedLane(expected.laneId(), maxRecords));
    }

    /** Applies an owner/runtime readiness transition without changing admission semantics. */
    synchronized LaneRecord updateLaneReadiness(
            final com.nereusstream.delay.protocol.DestinationLaneId laneId, final RuntimeReadiness readiness) {
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

    /**
     * Applies a strict typed Lane activation. The caller must supply the
     * exact Channel Resource, Ready Certificate and evidence cursor set
     * produced by the external prerequisite authority. This is intentionally
     * separate from the runtime-package readiness test seam above: a raw enum
     * can never make a typed Lane schedulable.
     */
    public synchronized LaneRecord activateLaneReadiness(
            final com.nereusstream.delay.protocol.DestinationLaneId laneId,
            final byte[] laneIncarnation,
            final ChannelResourceIdentity channel,
            final ReadyCertificate readyCertificate,
            final List<EvidenceCursor> evidenceCursors) {
        final DestinationLaneId exactLaneId = Objects.requireNonNull(laneId, "laneId");
        final byte[] exactIncarnation = Bytes.copy(Objects.requireNonNull(laneIncarnation, "laneIncarnation"));
        final ChannelResourceIdentity exactChannel = Objects.requireNonNull(channel, "channel");
        final ReadyCertificate exactCertificate = Objects.requireNonNull(readyCertificate, "readyCertificate");
        final List<EvidenceCursor> exactEvidence =
                List.copyOf(Objects.requireNonNull(evidenceCursors, "evidenceCursors"));
        final ActiveLaneState currentTyped = getActiveLaneState(exactLaneId);
        if (currentTyped == null) {
            throw new IllegalStateException("strict Lane activation requires a typed active Lane");
        }
        if (!Arrays.equals(currentTyped.laneIncarnation(), exactIncarnation)
                || !Arrays.equals(exactChannel.destinationLaneId(), exactLaneId.bytes())
                || !Arrays.equals(exactChannel.laneIncarnation(), exactIncarnation)) {
            throw new IllegalArgumentException("Lane activation identity does not match the typed Lane");
        }
        final ChannelResourceIdentity certificateChannel = ChannelResourceIdentity.decode(exactCertificate.channel());
        if (!exactChannel.equals(certificateChannel)
                || !Arrays.equals(exactCertificate.destinationLaneId(), exactLaneId.bytes())
                || !Arrays.equals(exactCertificate.laneIncarnation(), exactIncarnation)
                || !Arrays.equals(exactCertificate.storeIncarnation(), storeIncarnation())
                || !exactEvidence.equals(exactCertificate.evidenceCursors())) {
            throw new IllegalArgumentException("Lane activation proof is not self-consistent");
        }
        final CanonicalLaneTuple.Projection tuple = CanonicalLaneTuple.project(currentTyped.canonicalLaneTuple());
        if (!tuple.destinationProfile().equals(currentTyped.destinationProfile())
                || !tuple.capabilityProfile().equals(currentTyped.capabilityProfile())
                || !tuple.targetResource().equals(exactChannel.targetResource())
                || tuple.physicalPartition() != exactChannel.physicalPartition()
                || !tuple.targetResource()
                        .equals(exactCertificate.activationBarrier().resource())
                || tuple.physicalPartition()
                        != exactCertificate.activationBarrier().partition()) {
            throw new IllegalArgumentException("Lane activation proof does not match the pinned Lane tuple");
        }
        for (EvidenceCursor cursor : exactEvidence) {
            if (!Arrays.equals(cursor.destinationLaneId(), exactLaneId.bytes())
                    || !Arrays.equals(cursor.laneIncarnation(), exactIncarnation)) {
                throw new IllegalArgumentException("Lane activation evidence belongs to another Lane");
            }
        }

        final LaneValue currentValue = readLaneValue(exactLaneId);
        final LaneRecord current = currentValue.asLaneRecord();
        if (current.admissionGate() != AdmissionGate.OPEN) {
            throw new IllegalStateException("non-open Lane cannot become READY");
        }
        if (current.runtimeReadiness() == RuntimeReadiness.READY) {
            if (!Arrays.equals(currentTyped.readyCertificate(), exactCertificate.canonicalBytes())) {
                throw new IllegalStateException("READY Lane already carries another certificate");
            }
            return current;
        }
        if (current.runtimeReadiness() != RuntimeReadiness.RECOVERING_EVIDENCE) {
            throw new IllegalStateException("Lane must return to RECOVERING_EVIDENCE before activation");
        }
        final LaneRecord next = current.withReadiness(RuntimeReadiness.READY);
        final TimelineCandidate candidate = findLaneCandidate(exactLaneId, null, -1, null, null);
        final LaneProjection projection = projectLane(exactLaneId, current, next, candidate);
        store.write(batch -> {
            deleteReadyKey(batch, current);
            putReadyProjection(batch, projection, exactCertificate);
        });
        return projection.lane();
    }

    /** Applies a local management-gate transition with an exact CAS version. */
    synchronized LaneRecord updateLaneGate(
            final com.nereusstream.delay.protocol.DestinationLaneId laneId,
            final long expectedLaneControlVersion,
            final AdmissionGate gate) {
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
     * the same {@code meta_cf/LANE} key. The caller supplies the already
     * applied retirement progress and must invoke this only after the
     * Recovery-Floor and external-channel checks have passed.
     */
    synchronized LaneTerminalGuard retireLaneWithTerminalGuard(
            final com.nereusstream.delay.protocol.DestinationLaneId laneId,
            final long expectedLaneControlVersion,
            final LaneRetirementProgress progress,
            final LaneTerminalGuard guard) {
        Objects.requireNonNull(laneId, "laneId");
        Objects.requireNonNull(progress, "progress");
        Objects.requireNonNull(guard, "guard");
        final LaneValue currentValue = readLaneValue(laneId);
        if (currentValue == null || !currentValue.isActive()) {
            throw new IllegalStateException("lane is already terminal or missing");
        }
        final LaneRecord current = currentValue.asLaneRecord();
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
        if (currentValue.typedActiveState() != null) {
            final ActiveLaneState typed = currentValue.typedActiveState();
            if (!typed.destinationProfile().equals(guard.destinationProfile())
                    || !typed.capabilityProfile().equals(guard.capabilityProfile())
                    || !Arrays.equals(typed.canonicalLaneTuple(), guard.canonicalLaneTuple())
                    || !Arrays.equals(typed.canonicalLaneTupleSha256(), guard.canonicalLaneTupleSha256())) {
                throw new IllegalStateException("terminal guard does not match typed Lane identity");
            }
        }
        if (lastAppliedSourcePosition == null
                || !isAtOrBeforeExact(progress.intentSourcePosition(), lastAppliedSourcePosition)
                || !isAtOrAfterExact(guard.terminalSourcePosition(), progress.intentSourcePosition())) {
            throw new IllegalStateException("retirement progress is not source-ordered and applied");
        }
        if (findLaneCandidate(laneId, null, -1, null, null) != null || hasLaneRuntimeWork(laneId)) {
            throw new IllegalStateException("lane still has pending or inflight work");
        }
        final ShardQuota nextQuota = quota.removeLane();
        final LaneQuotaUsageProjection nextLaneQuota =
                laneQuotaUsage.removeLane(laneId, current.laneIncarnation(), Math.max(1, nextQuota.usageRevision()));
        store.write(batch -> {
            deleteReadyKey(batch, current);
            batch.putValue(
                    ColumnFamily.META,
                    2,
                    KeyCodec.metaLane(laneId),
                    LaneRecordEnvelope.terminal(guard).canonicalBytes());
            persistQuota(batch, nextQuota, nextLaneQuota);
        });
        quota = nextQuota;
        laneQuotaUsage = nextLaneQuota;
        return guard;
    }

    private static boolean isAtOrBeforeExact(final SourcePosition candidate, final SourcePosition upperBound) {
        final int order = candidate.compareTo(upperBound);
        return order < 0 || (order == 0 && Arrays.equals(candidate.canonicalBytes(), upperBound.canonicalBytes()));
    }

    private static boolean isAtOrAfterExact(final SourcePosition candidate, final SourcePosition lowerBound) {
        final int order = candidate.compareTo(lowerBound);
        return order > 0 || (order == 0 && Arrays.equals(candidate.canonicalBytes(), lowerBound.canonicalBytes()));
    }

    /** Returns the terminal guard at the Lane key, or {@code null} while active. */
    public synchronized LaneTerminalGuard getLaneTerminalGuard(
            final com.nereusstream.delay.protocol.DestinationLaneId laneId) {
        final LaneValue value = readLaneValue(laneId);
        return value == null || value.isActive() ? null : value.terminalGuard();
    }

    public synchronized SourcePosition lastAppliedSourcePosition() {
        return lastAppliedSourcePosition;
    }

    /**
     * Persists the owner epoch observed for the next activation boundary.
     *
     * <p>The epoch is Store metadata rather than a Shard Log mutation: it
     * records which owner opened this physical Store Incarnation and must be
     * durable before the surrounding owner gate becomes
     * {@code ACTIVE_FOR_COMMANDS}. The Store enforces the non-decreasing
     * unsigned-u64 rule and keeps the write synchronous.</p>
     */
    public synchronized void recordOpenedOwnerEpoch(final long ownerEpoch) {
        store.recordOpenedOwnerEpoch(ownerEpoch);
    }

    /** Returns the source-ordered ingress retry deadline, or {@code -1} before the first fence. */
    public synchronized long closedIngressDeadlineThrough() {
        return closedIngressDeadlineThrough;
    }

    public com.nereusstream.delay.protocol.ShardId shardId() {
        return store.shardId();
    }

    /** Binds owner-side work execution to this shard Store's exact Worker resource graph. */
    public void bindWorkClassExecutionRegistry(final WorkClassExecutionRegistry registry) {
        store.sharedResources().bindWorkClassExecutionRegistry(registry);
    }

    /** Returns the immutable local Store Incarnation used by Claim/Admission identity checks. */
    public synchronized byte[] storeIncarnation() {
        return Bytes.copy(store.metadata().storeIncarnation());
    }

    /** Returns the shard-bound control snapshot required by strict activation. */
    public synchronized CompatibleControlSnapshot controlSnapshot() {
        return store.controlSnapshot();
    }

    /** Returns the durable source-ordered Protocol Version marker projection. */
    public synchronized ProtocolActivationState protocolActivationState() {
        return protocolActivationState;
    }

    public synchronized long mutationSequence() {
        return mutationSequence;
    }

    public synchronized ShardQuota quota() {
        return quota;
    }

    /** Returns the canonical local per-Lane quota projection. */
    public synchronized com.nereusstream.delay.protocol.LaneQuotaUsageMap laneQuotaUsage() {
        return laneQuotaUsage.map();
    }

    /** Returns the persisted non-borrowable outcome reserve usage projection. */
    public synchronized OutcomeReserveUsage outcomeReserve() {
        return outcomeReserve;
    }

    /** Returns the exact 66-dimensional outcome grant usage when an envelope is bound. */
    public synchronized CapacityVector outcomeReserveVector() {
        return outcomeReserveVector;
    }

    /** Returns the Registry class-2 aggregate for locally accounted dimensions. */
    public synchronized CapacityVector quotaAggregateUsage() {
        return aggregateQuotaUsage(laneQuotaUsage, outcomeReserveVector);
    }

    /** Returns the immutable placement envelope bound to this shard, if one was supplied. */
    public ShardCapacityEnvelope capacityEnvelope() {
        return capacityEnvelope;
    }

    /** Returns the persisted usage of a control reserve class (3-6). */
    public synchronized CapacityVector controlReserveUsage(final int reserveClass) {
        validateMutableControlReserveClass(reserveClass);
        return controlReserveUsage.getOrDefault(reserveClass, CapacityVector.empty());
    }

    /** Returns the local class-6 projection for the Route Broker system writer. */
    public synchronized CapacityVector systemWriterReserveUsage() {
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
    synchronized CapacityVector reserveControlCapacity(final int reserveClass, final CapacityVector amount) {
        return mutateControlReserve(reserveClass, Objects.requireNonNull(amount, "amount"), true);
    }

    /** Charges the local class-6 system-writer projection. */
    synchronized CapacityVector reserveSystemWriterCapacity(final CapacityVector amount) {
        return reserveControlCapacity(CONTROL_RESERVE_SYSTEM_WRITER_CLASS, amount);
    }

    /** Releases an exact checked class-3/4/5/6 control reserve projection. */
    synchronized CapacityVector releaseControlCapacity(final int reserveClass, final CapacityVector amount) {
        return mutateControlReserve(reserveClass, Objects.requireNonNull(amount, "amount"), false);
    }

    /** Releases an exact local class-6 system-writer projection. */
    synchronized CapacityVector releaseSystemWriterCapacity(final CapacityVector amount) {
        return releaseControlCapacity(CONTROL_RESERVE_SYSTEM_WRITER_CLASS, amount);
    }

    /** Package-local compatibility scan; production scheduling uses bounded READY discovery. */
    synchronized List<TimelineWork> discoverDue(final long earliestEpochMs, final int limit) {
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
     * Returns the bounded READY head projection. A malformed, orphaned, or
     * version-mismatched entry fences discovery instead of silently falling
     * back to a full timeline scan.
     */
    synchronized List<ReadyWork> discoverReady(final long earliestEpochMs, final int limit) {
        if (earliestEpochMs < 0 || limit <= 0) {
            throw new IllegalArgumentException("invalid READY discovery bounds");
        }
        final List<ReadyWork> result = new ArrayList<>();
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> entries =
                store.scan(ColumnFamily.TIMELINE, new byte[] {3, 1}, new byte[] {4, 1}, limit);
        for (var entry : entries) {
            final ReadyKey key = decodeReadyKey(entry.key());
            final ReadyIndexValue value =
                    ReadyIndexValue.decode(com.nereusstream.delay.store.ValueEnvelope.decode(entry.value(), 3)
                            .payload());
            if (!key.laneId().equals(value.laneId())
                    || key.nextEligibleAtEpochMs() != value.nextEligibleAtEpochMs()
                    || key.laneVersion() != value.laneVersion()) {
                throw new IllegalStateException("READY key/value identity mismatch");
            }
            if (key.nextEligibleAtEpochMs() > earliestEpochMs) {
                break;
            }
            final LaneValue laneValue = readLaneValue(key.laneId());
            final LaneRecord lane = laneValue == null || !laneValue.isActive() ? null : laneValue.asLaneRecord();
            if (lane == null
                    || !lane.schedulable()
                    || lane.laneVersion() != key.laneVersion()
                    || lane.nextEligibleAtEpochMs() != key.nextEligibleAtEpochMs()) {
                throw new IllegalStateException("stale READY lane projection");
            }
            final MessageRecord message = getMessage(value.messageId());
            if (message == null
                    || message.status() != MessageStatus.SCHEDULED
                    || message.generation() != value.generation()
                    || !message.laneId().equals(key.laneId())) {
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
            final TimelineWorkRef work = validateTimelineValue(
                    timelineValue.payload(), value.messageId(), message, timelineKey, "READY discovery");
            if (work != null
                    && key.nextEligibleAtEpochMs()
                            != Math.max(work.actionAtEpochMs(), work.retryEligibilityAtEpochMs())) {
                throw new IllegalStateException("READY eligibility disagrees with TimelineWorkRef");
            }
            validateTypedReadyTimes(laneValue, value.messageId(), message, work, key.nextEligibleAtEpochMs());
            result.add(new ReadyWork(
                    key.laneId(),
                    value.messageId(),
                    value.generation(),
                    key.nextEligibleAtEpochMs(),
                    key.laneVersion(),
                    message.orderingMode() == com.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO));
        }
        return List.copyOf(result);
    }

    private void validateTypedReadyTimes(
            final LaneValue laneValue,
            final DelayMessageId messageId,
            final MessageRecord message,
            final TimelineWorkRef work,
            final long expectedNextEligibleAtEpochMs) {
        final ActiveLaneState state = laneValue.typedActiveState();
        if (state == null) {
            return;
        }
        final long actionAt = work == null ? actionAtFor(messageId, message) : work.actionAtEpochMs();
        if (state.earliestActionAtEpochMs() == null
                || state.earliestActionAtEpochMs() != actionAt
                || state.nextEligibleAtEpochMs() == null
                || state.nextEligibleAtEpochMs() != expectedNextEligibleAtEpochMs) {
            throw new IllegalStateException("typed READY action/eligibility projection disagrees with current head");
        }
    }

    /**
     * Rebuilds all READY projections while the shard is fenced. This is the
     * deterministic repair path for startup/recovery; normal command and
     * readiness mutations update the affected projection in their own batch.
     *
     * @return number of schedulable lanes that received a READY key
     */
    synchronized int rebuildReadyIndexes() {
        final int laneLimit = boundedLimitPlusOne(config.maxLanes());
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> laneEntries =
                store.scan(ColumnFamily.META, new byte[] {2, 1}, new byte[] {3, 1}, laneLimit);
        if (laneEntries.size() >= laneLimit && config.maxLanes() < Integer.MAX_VALUE) {
            throw new IllegalStateException("lane metadata exceeds configured maxLanes");
        }
        final Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneRecord> lanes = new HashMap<>();
        for (var entry : laneEntries) {
            final byte[] key = entry.key();
            if (key.length != 2 + 32 || key[0] != 2 || key[1] != 1) {
                throw new IllegalStateException("invalid lane metadata key");
            }
            final com.nereusstream.delay.protocol.DestinationLaneId laneId =
                    new com.nereusstream.delay.protocol.DestinationLaneId(Arrays.copyOfRange(key, 2, 34));
            final LaneValue laneValue =
                    decodeLaneValue(com.nereusstream.delay.store.ValueEnvelope.decode(entry.value(), 2)
                            .payload());
            if (!laneValue.isActive()) {
                continue;
            }
            final LaneRecord lane = laneValue.asLaneRecord();
            if (!lane.laneId().equals(laneId) || lanes.put(laneId, lane) != null) {
                throw new IllegalStateException("duplicate or mismatched lane metadata");
            }
        }
        final Map<com.nereusstream.delay.protocol.DestinationLaneId, TimelineCandidate> candidates = new HashMap<>();
        for (var laneId : lanes.keySet()) {
            final TimelineCandidate candidate = findLaneCandidate(laneId, null, -1, null, null);
            if (candidate != null) {
                candidates.put(laneId, candidate);
            }
        }
        final int readyLimit = boundedLimitPlusOne(config.maxLanes());
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> existingReady =
                store.scan(ColumnFamily.TIMELINE, new byte[] {3, 1}, new byte[] {4, 1}, readyLimit);
        if (existingReady.size() >= readyLimit && config.maxLanes() < Integer.MAX_VALUE) {
            throw new IllegalStateException("READY index exceeds configured maxLanes");
        }
        final Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections = new HashMap<>();
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
        return (int) projections.values().stream()
                .filter(projection -> projection.readyValue() != null)
                .count();
    }

    /** Returns expiry candidates; the caller must apply an exact source-ordered expiry mutation. */
    synchronized List<ExpiryWork> discoverExpiry(final long earliestEpochMs, final int limit) {
        return discoverExpiry(earliestEpochMs, new SchedulerBudget(limit, Long.MAX_VALUE, Long.MAX_VALUE), () -> 0);
    }

    /** Returns expiry candidates under record, actual-byte and elapsed scan bounds. */
    public synchronized List<ExpiryWork> discoverExpiry(
            final long earliestEpochMs, final SchedulerBudget budget, final LongSupplier monotonicClockNanos) {
        final SchedulerBudget bounded = Objects.requireNonNull(budget, "expiry discovery budget");
        final LongSupplier clock = Objects.requireNonNull(monotonicClockNanos, "expiry discovery monotonic clock");
        if (earliestEpochMs < 0) {
            throw new IllegalArgumentException("invalid expiry discovery bounds");
        }
        final List<ExpiryWork> result = new ArrayList<>();
        final BoundedReadBudget readBudget =
                new BoundedReadBudget(bounded.maxBytes(), bounded.maxElapsedNanos(), clock);
        final long[] completedCharge = {0};
        store.visit(
                ColumnFamily.TIMELINE,
                new byte[] {4, 1},
                new byte[] {5, 1},
                bounded.maxMessages(),
                readBudget,
                (entry, sharedBudget) -> {
                    final long indexCharge = sharedBudget.chargedBytes() - completedCharge[0];
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
                        return false;
                    }
                    final DelayMessageId messageId = new DelayMessageId(messageBytes);
                    final com.nereusstream.delay.protocol.DestinationLaneId laneId =
                            new com.nereusstream.delay.protocol.DestinationLaneId(laneBytes);
                    if (!sharedBudget.beforeRead()) {
                        return false;
                    }
                    final byte[] messageKey = KeyCodec.idMessage(messageId);
                    final byte[] rawMessage = store.get(ColumnFamily.ID, messageKey);
                    final int messageBytesLength = rawMessage == null ? 0 : rawMessage.length;
                    if (!sharedBudget.tryCharge(messageKey.length, messageBytesLength)) {
                        final long candidateBytes =
                                Math.addExact(indexCharge, Math.addExact((long) messageKey.length, messageBytesLength));
                        if (candidateBytes > sharedBudget.maxBytes()) {
                            throw new IllegalStateException("EXPIRY discovery candidate exceeds byte budget");
                        }
                        return false;
                    }
                    final MessageRecord message = rawMessage == null
                            ? null
                            : validateMessageSourcePosition(
                                    messageId,
                                    MessageRecord.decode(ValueEnvelope.decode(rawMessage, MESSAGE_VALUE_TYPE)
                                            .payload()),
                                    "EXPIRY discovery");
                    if (message == null
                            || (message.status() != MessageStatus.SCHEDULED
                                    && message.status() != MessageStatus.CLAIMED)
                            || message.generation() != generation
                            || message.expireAtEpochMs() != expireAt
                            || !message.laneId().equals(laneId)
                            || !Arrays.equals(entry.key(), expiryKey(messageId, message))) {
                        throw new IllegalStateException("EXPIRY timeline does not match the current message");
                    }
                    validateTimelineValue(
                            com.nereusstream.delay.store.ValueEnvelope.decode(entry.value(), 1)
                                    .payload(),
                            messageId,
                            message,
                            timelineKey(messageId, message),
                            message.status() == MessageStatus.SCHEDULED,
                            "EXPIRY discovery");
                    result.add(new ExpiryWork(messageId, laneId, generation, expireAt));
                    completedCharge[0] = sharedBudget.chargedBytes();
                    return true;
                });
        return List.copyOf(result);
    }

    /** Returns only reservations whose expiry is already decided by TIME_FENCE. */
    synchronized List<ReservationExpiryWork> discoverReservationExpiry(final long earliestEpochMs, final int limit) {
        if (earliestEpochMs < 0 || limit <= 0) {
            throw new IllegalArgumentException("invalid reservation expiry discovery bounds");
        }
        return discoverReservationExpiry(
                earliestEpochMs, new SchedulerBudget(limit, Long.MAX_VALUE, Long.MAX_VALUE), () -> 0);
    }

    /**
     * Discovers source-fence-decided reservation expiry under record, actual-byte
     * and elapsed scan bounds. The cutoff is the persisted TIME_FENCE watermark,
     * never caller-provided wall-clock time.
     */
    public synchronized List<ReservationExpiryWork> discoverReservationExpiry(
            final SchedulerBudget budget, final LongSupplier monotonicClockNanos) {
        final SchedulerBudget bounded = Objects.requireNonNull(budget, "reservation expiry discovery budget");
        final LongSupplier clock =
                Objects.requireNonNull(monotonicClockNanos, "reservation expiry discovery monotonic clock");
        if (closedIngressDeadlineThrough < 0) {
            return List.of();
        }
        return discoverReservationExpiry(closedIngressDeadlineThrough, bounded, clock);
    }

    private List<ReservationExpiryWork> discoverReservationExpiry(
            final long cutoffEpochMs, final SchedulerBudget budget, final LongSupplier monotonicClockNanos) {
        final List<ReservationExpiryWork> result = new ArrayList<>();
        final BoundedReadBudget readBudget =
                new BoundedReadBudget(budget.maxBytes(), budget.maxElapsedNanos(), monotonicClockNanos);
        final long[] completedCharge = {0};
        store.visit(
                ColumnFamily.TIMELINE,
                new byte[] {5, 1},
                new byte[] {6, 1},
                budget.maxMessages(),
                readBudget,
                (entry, sharedBudget) -> {
                    final long indexCharge = sharedBudget.chargedBytes() - completedCharge[0];
                    final byte[] key = entry.key();
                    if (key.length != 2 + 8 + 32 || key[0] != 5 || key[1] != 1) {
                        throw new IllegalStateException("invalid RESERVATION_EXPIRY key");
                    }
                    final ByteBuffer input = ByteBuffer.wrap(key);
                    input.position(2);
                    final long expiry = input.getLong();
                    final byte[] reservationId = new byte[32];
                    input.get(reservationId);
                    if (expiry > cutoffEpochMs) {
                        return false;
                    }
                    final PayloadReservation reservation = validateReservationIdentity(
                            reservationId,
                            PayloadReservation.decode(
                                    ValueEnvelope.decode(entry.value(), 5).payload()),
                            "RESERVATION_EXPIRY discovery");
                    if (!Arrays.equals(reservation.reservationId(), reservationId)
                            || reservation.reservationExpiryEpochMs() != expiry) {
                        throw new IllegalStateException("RESERVATION_EXPIRY key/value identity mismatch");
                    }
                    if (!sharedBudget.beforeRead()) {
                        return false;
                    }
                    final byte[] reservationKey = KeyCodec.idReservation(reservationId);
                    final byte[] rawReservation = store.get(ColumnFamily.ID, reservationKey);
                    final int reservationValueBytes = rawReservation == null ? 0 : rawReservation.length;
                    if (!sharedBudget.tryCharge(reservationKey.length, reservationValueBytes)) {
                        final long candidateBytes = Math.addExact(
                                indexCharge, Math.addExact((long) reservationKey.length, reservationValueBytes));
                        if (candidateBytes > sharedBudget.maxBytes()) {
                            throw new IllegalStateException(
                                    "RESERVATION_EXPIRY discovery candidate exceeds byte budget");
                        }
                        return false;
                    }
                    final PayloadReservation current = rawReservation == null
                            ? null
                            : validateReservationIdentity(
                                    reservationId,
                                    PayloadReservation.decode(ValueEnvelope.decode(rawReservation, 2)
                                            .payload()),
                                    "RESERVATION_EXPIRY current projection");
                    if (current == null || !Arrays.equals(current.encode(), reservation.encode())) {
                        throw new IllegalStateException("RESERVATION_EXPIRY is not the current reservation projection");
                    }
                    if (effectiveReservation(reservation).status() == PayloadReservationStatus.EXPIRED) {
                        result.add(new ReservationExpiryWork(
                                reservation.reservationId(),
                                reservation.delayMessageId(),
                                reservation.reservationExpiryEpochMs(),
                                reservation.stateVersion()));
                    }
                    completedCharge[0] = sharedBudget.chargedBytes();
                    return true;
                });
        return List.copyOf(result);
    }

    private CommandResult applyLargePayloadCommand(final PreparedCommand command, final SourcePosition sourcePosition) {
        try {
            return command.type() == com.nereusstream.delay.protocol.CommandType.PREPARE_LARGE_SCHEDULE
                    ? applyPrepareLarge(command, sourcePosition)
                    : applyCommitLarge(command, sourcePosition);
        } catch (WindowViolationException exception) {
            return persistRejected(command, sourcePosition, StableCode.INVALID_DELIVERY_WINDOW);
        } catch (CommandResolutionException exception) {
            return persistRejected(command, sourcePosition, exception.stableCode());
        } catch (ArithmeticException | IllegalArgumentException exception) {
            return persistRejected(command, sourcePosition, StableCode.INVALID_COMMAND);
        }
    }

    private LargeScheduleIntent decodePrepareLargeIntent(
            final PreparedCommand command, final SourcePosition sourcePosition) {
        if (!CommandBodies.isRegistryClientBody(command.canonicalBody())) {
            return CommandBodies.decodeDirectPrepareLarge(command.canonicalBody());
        }
        final PrepareLargeScheduleBody body = CommandBodies.decodePrepareLarge(command.canonicalBody());
        requireBodyIdentity(command, body.delayMessageId(), body.retryUntilEpochMs());
        requireProfileFirstBinding(body.intentWithoutPayload().profile(), sourcePosition);
        requireProfileFirstBinding(body.objectStoreProfile(), sourcePosition);
        requireObjectStorePayloadLimit(body.objectStoreProfile(), body.expectedPayloadLength());
        requireRetryPolicy(
                body.intentWithoutPayload().retryPolicy(),
                body.intentWithoutPayload().orderingMode(),
                sourcePosition);
        final ScheduleResolver resolver = requireScheduleResolver();
        final ScheduleResolver.ResolvedPrepare resolved = Objects.requireNonNull(
                resolver.resolvePrepare(command.shardId(), command.delayMessageId(), body, sourcePosition),
                "resolved PrepareLargeSchedule projection");
        if (payloadProofTrustSetControlCatalog != null
                && !payloadProofTrustSetControlState.activatedAt(body.trustSet(), sourcePosition)) {
            throw new CommandResolutionException(
                    StableCode.PAYLOAD_PROOF_KEY_NOT_AUTHORIZED_AT_SOURCE_POSITION,
                    "PrepareLargeSchedule trust set is not active at its source position");
        }
        lastResolvedPrepare = resolved;
        final CanonicalScheduleIntent intent = body.intentWithoutPayload();
        return new LargeScheduleIntent(
                resolved.laneId(),
                intent.deliverAtEpochMs(),
                intent.expireAtEpochMs(),
                intent.orderingMode(),
                body.expectedPayloadLength(),
                body.payloadSha256(),
                body.reservationTtlMs(),
                body.trustSet().version());
    }

    /** Requires the exact Object Store semantic/current Head and its immutable object-size bound. */
    private void requireObjectStorePayloadLimit(final ProfileRef reference, final long payloadLength) {
        if (profileCatalog == null) {
            return;
        }
        final ProfileSemanticEnvelope semantic = profileCatalog.resolve(reference);
        final CredentialBindingHead head = profileCatalog.resolveHead(reference);
        if (semantic == null
                || !semantic.ref().equals(reference)
                || !(semantic.body() instanceof ObjectStoreProfileSemantic objectStore)
                || head == null
                || !head.profile().equals(reference)) {
            throw new CommandResolutionException(
                    StableCode.ROUTE_SNAPSHOT_UNAVAILABLE,
                    "Object Store Profile semantic or credential Head is unavailable");
        }
        if (payloadLength > objectStore.maxObjectBytes()) {
            throw new CommandResolutionException(
                    StableCode.PAYLOAD_TOO_LARGE, "payload exceeds the immutable Object Store Profile maximum");
        }
    }

    private ScheduleApplication decodeScheduleApplication(
            final PreparedCommand command, final SourcePosition sourcePosition) {
        if (!CommandBodies.isRegistryClientBody(command.canonicalBody())) {
            final var direct = CommandBodies.decodeDirectSchedule(command.canonicalBody());
            return new ScheduleApplication(
                    direct.deliverAtEpochMs(),
                    direct.expireAtEpochMs(),
                    direct.deliverAtEpochMs(),
                    direct.laneId(),
                    direct.orderingMode(),
                    direct.payload(),
                    null,
                    NativeDeliveryPolicy.FORBID);
        }
        final ScheduleCommandBody body = CommandBodies.decodeSchedule(command.canonicalBody());
        requireBodyIdentity(command, body.delayMessageId(), body.retryUntilEpochMs());
        requireProfileFirstBinding(body.intent().profile(), sourcePosition);
        if (!body.intent().hasInlinePayload()) {
            requireProfileFirstBinding(body.intent().committedPayload().objectStoreProfile(), sourcePosition);
            requireObjectStorePayloadLimit(
                    body.intent().committedPayload().objectStoreProfile(),
                    body.intent().committedPayload().length());
        }
        requireRetryPolicy(body.intent().retryPolicy(), body.intent().orderingMode(), sourcePosition);
        final ScheduleResolver resolver = requireScheduleResolver();
        final ScheduleResolver.ResolvedSchedule resolved = Objects.requireNonNull(
                resolver.resolveSchedule(command.shardId(), command.delayMessageId(), body.intent(), sourcePosition),
                "resolved Schedule projection");
        lastResolvedSchedule = resolved;
        lastResolvedScheduleMessageId = command.delayMessageId();
        validateResolvedSchedulePayload(body.intent(), resolved);
        return new ScheduleApplication(
                body.intent().deliverAtEpochMs(),
                body.intent().expireAtEpochMs(),
                resolved.actionAtEpochMs() == null ? body.intent().deliverAtEpochMs() : resolved.actionAtEpochMs(),
                resolved.laneId(),
                body.intent().orderingMode(),
                resolved.inlinePayload() == null ? new byte[0] : resolved.inlinePayload(),
                resolved.payloadReference(),
                body.intent().nativeDeliveryPolicy());
    }

    private ScheduleResolver requireScheduleResolver() {
        if (scheduleResolver == null) {
            throw new CommandResolutionException(
                    StableCode.ROUTE_SNAPSHOT_UNAVAILABLE,
                    " Schedule/Prepare requires a source-position-pinned resolver");
        }
        return scheduleResolver;
    }

    private void requireRetryPolicy(
            final RetryPolicyRef reference,
            final com.nereusstream.delay.protocol.OrderingMode orderingMode,
            final SourcePosition sourcePosition) {
        if (retryPolicyCatalog == null) {
            return;
        }
        final RetryPolicySemantic semantic = retryPolicyCatalog.resolve(
                Objects.requireNonNull(reference, "reference"),
                Objects.requireNonNull(sourcePosition, "sourcePosition"));
        if (semantic == null) {
            throw new CommandResolutionException(
                    StableCode.RETRY_POLICY_NOT_ACTIVE_AT_SOURCE_POSITION,
                    "Retry Policy is not active at the command Source Position");
        }
        if (!reference.matches(semantic)) {
            throw new CommandResolutionException(
                    StableCode.INVALID_COMMAND, "Retry Policy reference does not match catalog semantic bytes");
        }
        try {
            semantic.validateFor(orderingMode);
        } catch (IllegalArgumentException exception) {
            throw new CommandResolutionException(
                    StableCode.INVALID_COMMAND, "Retry Policy is incompatible with the requested ordering mode");
        }
        ensureRetryPolicyFitsConfig(semantic);
    }

    private void ensureRetryPolicyFitsConfig(final RetryPolicySemantic semantic) {
        if (semantic.maxPublishAdmissions() > config.maxPublishAdmissions()
                || semantic.maxUncertainRetries() > config.maxUncertainRetries()) {
            throw new CommandResolutionException(
                    StableCode.INVALID_COMMAND, "local shard limits cannot honor the immutable Retry Policy budget");
        }
    }

    /** Resolves the policy pinned by an accepted binding for later replay turns. */
    private RetryPolicySemantic retryPolicyFor(
            final DelayMessageId messageId, final MessageRecord message, final SourcePosition sourcePosition) {
        if (retryPolicyCatalog == null) {
            return null;
        }
        final ScheduleBinding binding = getScheduleBinding(messageId);
        if (binding == null) {
            return null;
        }
        final RetryPolicyRef reference;
        if (binding.commandType() == com.nereusstream.delay.protocol.CommandType.SCHEDULE) {
            reference = CommandBodies.decodeSchedule(binding.canonicalBody())
                    .intent()
                    .retryPolicy();
        } else {
            reference = CommandBodies.decodePrepareLarge(binding.canonicalBody())
                    .intentWithoutPayload()
                    .retryPolicy();
        }
        final RetryPolicySemantic semantic = retryPolicyCatalog.resolve(reference, sourcePosition);
        if (semantic == null || !reference.matches(semantic)) {
            throw new CommandResolutionException(
                    StableCode.RETRY_POLICY_NOT_ACTIVE_AT_SOURCE_POSITION,
                    "pinned Retry Policy is unavailable or mismatched for replay");
        }
        try {
            semantic.validateFor(message.orderingMode());
        } catch (IllegalArgumentException exception) {
            throw new CommandResolutionException(
                    StableCode.INVALID_COMMAND, "pinned Retry Policy no longer matches message ordering");
        }
        ensureRetryPolicyFitsConfig(semantic);
        return semantic;
    }

    private static void validateResolvedSchedulePayload(
            final CanonicalScheduleIntent intent, final ScheduleResolver.ResolvedSchedule resolved) {
        if (intent.hasInlinePayload()) {
            if (resolved.payloadReference() != null
                    || !Arrays.equals(intent.inlinePayload(), resolved.inlinePayload())) {
                throw new CommandResolutionException(
                        StableCode.INVALID_COMMAND, "resolved inline payload does not match CanonicalScheduleIntent");
            }
            return;
        }
        final var descriptor = intent.committedPayload();
        final PayloadReference reference = resolved.payloadReference();
        if (reference == null
                || resolved.inlinePayload() != null
                || !Bytes.constantTimeEquals(
                        reference.objectStoreProfileHash(),
                        descriptor.objectStoreProfile().semanticHash())
                || !Arrays.equals(reference.container(), descriptor.container())
                || !Arrays.equals(reference.objectKey(), descriptor.objectKey())
                || !Arrays.equals(reference.immutableObjectVersion(), descriptor.immutableObjectVersion())
                || !optionalBytesEqual(reference.etag(), descriptor.etag())
                || reference.length() != descriptor.length()
                || !Bytes.constantTimeEquals(reference.payloadSha256(), descriptor.payloadSha256())) {
            throw new CommandResolutionException(
                    StableCode.INVALID_COMMAND, "resolved object payload does not match CanonicalScheduleIntent");
        }
    }

    private CommandResult applyPrepareLarge(final PreparedCommand command, final SourcePosition sourcePosition) {
        final LargeScheduleIntent intent = decodePrepareLargeIntent(command, sourcePosition);
        validateWindow(
                intent.deliverAtEpochMs(), intent.expireAtEpochMs(), sourcePosition.brokerPersistenceTimeEpochMs());
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
                    ? StableCode.LANE_TERMINALLY_CLOSED
                    : StableCode.LANE_CLOSED;
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
        final PayloadReservation reservation = new PayloadReservation(
                store.shardId(),
                reservationId,
                command.commandId(),
                command.delayMessageId(),
                command.commandHash(),
                intent,
                expiry,
                PayloadReservationStatus.RESERVED,
                1,
                sourcePosition.canonicalBytes(),
                null);
        final ShardQuota nextQuota = quota.addReservation(intent.expectedPayloadLength(), newLane);
        final CommandResult result = applied(StableCode.OK, sourcePosition, null);
        persistMutation(command, sourcePosition, result, null, reservation, nextQuota);
        return result;
    }

    private CommandResult applyCommitLarge(final PreparedCommand command, final SourcePosition sourcePosition) {
        final PayloadCommitProofView proof;
        if (CommandBodies.isRegistryClientBody(command.canonicalBody())) {
            final CommitLargeScheduleBody body = CommandBodies.decodeCommitLarge(command.canonicalBody());
            requireBodyIdentity(command, body.delayMessageId(), body.retryUntilEpochMs());
            proof = body.proof();
        } else {
            proof = CommandBodies.decodeDirectCommitLarge(command.canonicalBody());
        }
        final PayloadReservation storedReservation = readStoredReservation(proof.reservationId());
        if (storedReservation == null || !storedReservation.delayMessageId().equals(command.delayMessageId())) {
            return persistRejected(command, sourcePosition, StableCode.RESERVATION_NOT_COMMITTED);
        }
        final LaneRecord reservationLane = readLane(storedReservation.intent().laneId());
        if (reservationLane == null) {
            throw new IllegalStateException("large payload commit references a reservation on a missing Lane");
        }
        // A terminal guard is the irreversible identity fence for the old
        // Lane tuple. Check it before the reservation lifecycle: a stale
        // RESERVED/COMMITTED value must never project a new Message and
        // resurrect the compact terminal key as an ACTIVE Lane value.
        if (reservationLane.admissionGate() == AdmissionGate.RETIRED) {
            return persistRejected(command, sourcePosition, StableCode.LANE_TERMINALLY_CLOSED);
        }
        if (reservationLane.admissionGate() == AdmissionGate.CLOSED
                && storedReservation.status() == PayloadReservationStatus.RESERVED) {
            return persistRejected(command, sourcePosition, StableCode.PAYLOAD_RESERVATION_CLOSED);
        }
        // The durable Prepare binding chooses the Object Store authority for
        // every Commit attempt, including a retry after the reservation has
        // already reached COMMITTED. A lifecycle fast path must not weaken
        // that pinned identity boundary.
        final PrepareLargeScheduleBody pinnedPrepare = pinnedPrepareBodyIfPresent(command.delayMessageId());
        if (pinnedPrepare != null && !proofMatchesPinnedObjectStore(proof, pinnedPrepare.objectStoreProfile())) {
            return persistRejected(command, sourcePosition, StableCode.PAYLOAD_PROOF_INVALID);
        }
        final PayloadReservation reservation = effectiveReservation(storedReservation);
        if (reservation.status() == PayloadReservationStatus.COMMITTED) {
            if (reservation.committedPayload() != null && proofMatches(proof, reservation.committedPayload())) {
                if (!historicallyVerifies(proof, pinnedPrepare, sourcePosition)) {
                    return persistRejected(
                            command, sourcePosition, StableCode.PAYLOAD_PROOF_KEY_NOT_AUTHORIZED_AT_SOURCE_POSITION);
                }
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
                || !java.util.Arrays.equals(
                        proof.routeIncarnationUuid(),
                        store.shardId().routeIncarnation().bytes())
                || proof.partition() != store.shardId().partition()
                || !proof.delayMessageId().equals(command.delayMessageId())
                || proof.length() != reservation.intent().expectedPayloadLength()
                || !Bytes.constantTimeEquals(
                        proof.payloadSha256(), reservation.intent().payloadSha256())) {
            return persistRejected(command, sourcePosition, StableCode.PAYLOAD_PROOF_INVALID);
        }
        // A later legacy body can change only the proof encoding; it cannot
        // weaken the trust-set semantic reference pinned by Prepare.
        final PayloadProofTrustSetRef pinnedTrustSet = pinnedPrepare == null ? null : pinnedPrepare.trustSet();
        final boolean proofAuthorized;
        if (pinnedTrustSet != null) {
            if (payloadProofTrustSetControlCatalog == null) {
                throw new CommandResolutionException(
                        StableCode.ROUTE_SNAPSHOT_UNAVAILABLE, " payload proof trust-set catalog is unavailable");
            }
            if (proof.trustSetVersion() != pinnedTrustSet.version()
                    || !payloadProofTrustSetControlState.firstSeenIssuanceOpen(
                            pinnedTrustSet, proof.proofKeyVersion(), sourcePosition)) {
                return persistRejected(
                        command, sourcePosition, StableCode.PAYLOAD_PROOF_KEY_NOT_AUTHORIZED_AT_SOURCE_POSITION);
            }
            proofAuthorized = PayloadProofTrustSet.fromSemantic(requireTrustSetSemantic(pinnedTrustSet))
                    .verifies(proof, sourcePosition.brokerPersistenceTimeEpochMs());
        } else {
            proofAuthorized = payloadProofTrustSet != null
                    && payloadProofTrustSet.verifies(proof, sourcePosition.brokerPersistenceTimeEpochMs());
        }
        if (!proofAuthorized) {
            return persistRejected(
                    command, sourcePosition, StableCode.PAYLOAD_PROOF_KEY_NOT_AUTHORIZED_AT_SOURCE_POSITION);
        }
        final PayloadReference reference = new PayloadReference(
                proof.objectStoreProfileHash(),
                proof.container(),
                proof.objectKey(),
                proof.immutableObjectVersion(),
                proof.etag(),
                proof.length(),
                proof.payloadSha256(),
                proof.reservationId(),
                proof.proofId());
        final long actionAt = actionAtFor(
                command.delayMessageId(),
                MessageRecord.current(
                        MessageStatus.SCHEDULED,
                        0,
                        1,
                        reservation.intent().deliverAtEpochMs(),
                        reservation.intent().expireAtEpochMs(),
                        reservation.intent().laneId(),
                        reservation.intent().orderingMode(),
                        new byte[0],
                        sourcePosition.canonicalBytes(),
                        reference),
                reservation.intent().deliverAtEpochMs());
        final MessageRecord message = MessageRecord.current(
                MessageStatus.SCHEDULED,
                0,
                1,
                reservation.intent().deliverAtEpochMs(),
                reservation.intent().expireAtEpochMs(),
                reservation.intent().laneId(),
                reservation.intent().orderingMode(),
                new byte[0],
                sourcePosition.canonicalBytes(),
                reference,
                actionAt);
        final PayloadReservation committed = reservation.withLifecycle(
                PayloadReservationStatus.COMMITTED,
                Math.addExact(reservation.stateVersion(), 1),
                sourcePosition.canonicalBytes(),
                reference);
        final ShardQuota nextQuota = quota.commitReservation(reference.length());
        final CommandResult result = applied(StableCode.SCHEDULED, sourcePosition, message);
        persistMutation(command, sourcePosition, result, message, committed, nextQuota);
        return result;
    }

    private PrepareLargeScheduleBody pinnedPrepareBodyIfPresent(final DelayMessageId messageId) {
        final ScheduleBinding binding = getScheduleBinding(messageId);
        if (binding == null) {
            return null;
        }
        if (binding.commandType() != com.nereusstream.delay.protocol.CommandType.PREPARE_LARGE_SCHEDULE) {
            throw new CommandResolutionException(
                    StableCode.ROUTE_SNAPSHOT_UNAVAILABLE, "large payload reservation has a non-Prepare binding");
        }
        return CommandBodies.decodePrepareLarge(binding.canonicalBody());
    }

    private static boolean proofMatchesPinnedObjectStore(
            final PayloadCommitProofView proof, final ProfileRef pinnedProfile) {
        return proof instanceof CanonicalPayloadCommitProof typed
                ? typed.objectStoreProfile().equals(pinnedProfile)
                : Bytes.constantTimeEquals(proof.objectStoreProfileHash(), pinnedProfile.semanticHash());
    }

    private static byte[] reservationId(final PreparedCommand command) {
        return Bytes.sha256(
                Bytes.utf8("nereus-delay-reservation-id\0"),
                command.commandId().bytes(),
                command.delayMessageId().bytes(),
                command.commandHash());
    }

    private static boolean proofMatches(final PayloadCommitProofView proof, final PayloadReference reference) {
        return reference.hasCommitIdentity()
                && Bytes.constantTimeEquals(proof.reservationId(), reference.reservationId())
                && Bytes.constantTimeEquals(proof.proofId(), reference.proofId())
                && Bytes.constantTimeEquals(proof.objectStoreProfileHash(), reference.objectStoreProfileHash())
                && java.util.Arrays.equals(proof.container(), reference.container())
                && java.util.Arrays.equals(proof.objectKey(), reference.objectKey())
                && java.util.Arrays.equals(proof.immutableObjectVersion(), reference.immutableObjectVersion())
                && optionalBytesEqual(proof.etag(), reference.etag())
                && proof.length() == reference.length()
                && Bytes.constantTimeEquals(proof.payloadSha256(), reference.payloadSha256());
    }

    private boolean historicallyVerifies(
            final PayloadCommitProofView proof,
            final PrepareLargeScheduleBody pinnedPrepare,
            final SourcePosition sourcePosition) {
        if (pinnedPrepare == null) {
            return payloadProofTrustSet != null && payloadProofTrustSet.verifiesHistoricalSignature(proof);
        }
        final PayloadProofTrustSetRef pinnedTrustSet = pinnedPrepare.trustSet();
        if (payloadProofTrustSetControlCatalog == null) {
            throw new CommandResolutionException(
                    StableCode.ROUTE_SNAPSHOT_UNAVAILABLE, " payload proof trust-set catalog is unavailable");
        }
        return proof.trustSetVersion() == pinnedTrustSet.version()
                && payloadProofTrustSetControlState.historicalVerificationAllowed(
                        pinnedTrustSet, proof.proofKeyVersion(), sourcePosition)
                && PayloadProofTrustSet.fromSemantic(requireTrustSetSemantic(pinnedTrustSet))
                        .verifiesHistoricalSignature(proof);
    }

    private static boolean optionalBytesEqual(final byte[] left, final byte[] right) {
        final byte[] normalizedLeft = left == null || left.length == 0 ? null : left;
        final byte[] normalizedRight = right == null || right.length == 0 ? null : right;
        return Arrays.equals(normalizedLeft, normalizedRight);
    }

    private CommandResult applySchedule(final PreparedCommand command, final SourcePosition sourcePosition) {
        final ScheduleApplication intent = decodeScheduleApplication(command, sourcePosition);
        validateWindow(
                intent.deliverAtEpochMs(), intent.expireAtEpochMs(), sourcePosition.brokerPersistenceTimeEpochMs());
        final LaneRecord existingLane = readLane(intent.laneId());
        if (existingLane != null && existingLane.admissionGate() != AdmissionGate.OPEN) {
            final StableCode code = existingLane.admissionGate() == AdmissionGate.RETIRED
                    ? StableCode.LANE_TERMINALLY_CLOSED
                    : StableCode.LANE_CLOSED;
            return rejected(code, sourcePosition, -1, 0, null);
        }
        final MessageRecord existing = getMessage(command.delayMessageId());
        if (existing != null) {
            return rejected(
                    StableCode.DELAY_MESSAGE_ID_CONFLICT,
                    sourcePosition,
                    existing.generation(),
                    existing.stateVersion(),
                    existing.status());
        }
        if (getRetiredMessageIdentity(command.delayMessageId()) != null) {
            return rejected(StableCode.DELAY_MESSAGE_ID_CONFLICT, sourcePosition, -1, 0, null);
        }
        validateFirstSeenMessageIdentity(command.delayMessageId(), sourcePosition);
        if (closedIngressDeadlineThrough >= messageIdentityReuseUntil(command.delayMessageId())) {
            return rejected(StableCode.DELAY_MESSAGE_ID_EXPIRED, sourcePosition, -1, 0, null);
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
        final MessageRecord message = MessageRecord.current(
                MessageStatus.SCHEDULED,
                0,
                1,
                intent.deliverAtEpochMs(),
                intent.expireAtEpochMs(),
                intent.nativeDeliveryPolicy() == NativeDeliveryPolicy.FORBID
                        ? intent.deliverAtEpochMs()
                        : intent.actionAtEpochMs(),
                intent.laneId(),
                intent.orderingMode(),
                intent.nativeDeliveryPolicy(),
                intent.payload(),
                sourcePosition.canonicalBytes(),
                intent.payloadReference(),
                intent.actionAtEpochMs());
        return applied(StableCode.SCHEDULED, sourcePosition, message);
    }

    private void discoverDueNamespace(
            final byte tag,
            final byte nextTag,
            final long earliestEpochMs,
            final int limit,
            final List<TimelineWork> result) {
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> entries =
                store.scan(ColumnFamily.TIMELINE, new byte[] {tag, 1}, new byte[] {nextTag, 1}, limit - result.size());
        for (var entry : entries) {
            final byte[] key = entry.key();
            final int tokenLength = key.length > 2 + 32 + 8 && key[2 + 32 + 8] == 1
                    ? 9
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
            final DelayMessageId messageId = new DelayMessageId(messageBytes);
            final com.nereusstream.delay.protocol.DestinationLaneId laneId =
                    new com.nereusstream.delay.protocol.DestinationLaneId(laneBytes);
            final MessageRecord message = getMessage(messageId);
            if (message == null
                    || message.status() != MessageStatus.SCHEDULED
                    || message.generation() != generation
                    || !message.laneId().equals(laneId)
                    || !Arrays.equals(entry.key(), timelineKey(messageId, message))) {
                throw new IllegalStateException("DUE timeline does not match the current scheduled message");
            }
            validateTimelineValue(
                    com.nereusstream.delay.store.ValueEnvelope.decode(entry.value(), 1)
                            .payload(),
                    messageId,
                    message,
                    entry.key(),
                    "DUE discovery");
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
                if (!matchesPrecondition(
                        request.expectedGeneration(), request.expectedStateVersion(), 0, reservation.stateVersion())) {
                    return applied(StableCode.VERSION_CONFLICT, sourcePosition, null);
                }
                final LaneRecord reservationLane = readLane(reservation.intent().laneId());
                if (reservationLane == null) {
                    throw new IllegalStateException("cancel references a reservation on a missing Lane");
                }
                if (reservationLane.admissionGate() == AdmissionGate.CLOSED
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
        if (!matchesPrecondition(
                request.expectedGeneration(),
                request.expectedStateVersion(),
                existing.generation(),
                existing.stateVersion())) {
            return applied(StableCode.VERSION_CONFLICT, sourcePosition, existing);
        }
        final LaneRecord lane = readLane(existing.laneId());
        if (lane == null) {
            throw new IllegalStateException("cancel references a message on a missing Lane");
        }
        if (lane.admissionGate() == AdmissionGate.CLOSED && isUnadmittedGeneration(existing)) {
            return applied(StableCode.ALREADY_DEAD_LETTERED, sourcePosition, existing);
        }
        if ((existing.status() == MessageStatus.SCHEDULED || existing.status() == MessageStatus.CLAIMED)
                && hasUncertainObligation(existing.runtimeIndex())) {
            return applied(StableCode.TOO_LATE, sourcePosition, existing);
        }
        if (existing.status() == MessageStatus.DEAD_LETTER) {
            final TerminalGenerationRecord terminal =
                    getTerminalGeneration(command.delayMessageId(), existing.generation());
            if (terminal != null && terminal.terminalCode() == StableCode.LANE_CLOSED_BEFORE_ADMISSION) {
                return applied(StableCode.ALREADY_DEAD_LETTERED, sourcePosition, existing);
            }
        }
        return switch (existing.status()) {
            case SCHEDULED, CLAIMED ->
                applied(
                        StableCode.CANCELED,
                        sourcePosition,
                        MessageRecord.current(
                                MessageStatus.CANCELED,
                                existing.generation(),
                                Math.incrementExact(existing.stateVersion()),
                                existing.deliverAtEpochMs(),
                                existing.expireAtEpochMs(),
                                existing.laneId(),
                                existing.orderingMode(),
                                existing.payload(),
                                existing.scheduleSourcePosition(),
                                existing.payloadReference(),
                                existing.retryEligibilityAtEpochMs()));
            case CANCELED -> applied(StableCode.ALREADY_CANCELED, sourcePosition, existing);
            case PUBLISHED, HANDED_OFF, PUBLISHING, UNCERTAIN -> applied(StableCode.TOO_LATE, sourcePosition, existing);
            default -> applied(StableCode.TOO_LATE, sourcePosition, existing);
        };
    }

    private CommandResult applyReschedule(final PreparedCommand command, final SourcePosition sourcePosition) {
        final RescheduleRequest request = decodeRescheduleRequest(command);
        final MessageRecord existing = getMessage(command.delayMessageId());
        if (existing == null) {
            return applied(StableCode.NOT_FOUND, sourcePosition, null);
        }
        if (!matchesPrecondition(
                request.expectedGeneration(),
                request.expectedStateVersion(),
                existing.generation(),
                existing.stateVersion())) {
            return applied(StableCode.VERSION_CONFLICT, sourcePosition, existing);
        }
        final LaneRecord lane = readLane(existing.laneId());
        if (lane == null) {
            throw new IllegalStateException("reschedule references a message on a missing Lane");
        }
        if (lane.admissionGate() == AdmissionGate.CLOSED && isUnadmittedGeneration(existing)) {
            return applied(StableCode.LANE_CLOSED, sourcePosition, existing);
        }
        if ((existing.status() == MessageStatus.SCHEDULED || existing.status() == MessageStatus.CLAIMED)
                && hasUncertainObligation(existing.runtimeIndex())) {
            return applied(StableCode.TOO_LATE, sourcePosition, existing);
        }
        if (existing.status() == MessageStatus.DEAD_LETTER) {
            final TerminalGenerationRecord terminal =
                    getTerminalGeneration(command.delayMessageId(), existing.generation());
            if (terminal != null && terminal.terminalCode() == StableCode.LANE_CLOSED_BEFORE_ADMISSION) {
                return applied(StableCode.LANE_CLOSED, sourcePosition, existing);
            }
        }
        if (existing.status() != MessageStatus.SCHEDULED && existing.status() != MessageStatus.CLAIMED) {
            return applied(StableCode.TOO_LATE, sourcePosition, existing);
        }
        validateWindow(
                request.deliverAtEpochMs(), request.expireAtEpochMs(), sourcePosition.brokerPersistenceTimeEpochMs());
        final MessageRecord replacement = MessageRecord.current(
                MessageStatus.SCHEDULED,
                UnsignedInt32.successor(existing.generation()),
                Math.incrementExact(existing.stateVersion()),
                request.deliverAtEpochMs(),
                request.expireAtEpochMs(),
                existing.laneId(),
                existing.orderingMode(),
                existing.payload(),
                sourcePosition.canonicalBytes(),
                existing.payloadReference(),
                actionAtFor(command.delayMessageId(), existing, request.deliverAtEpochMs()));
        return applied(StableCode.SUPERSEDED, sourcePosition, replacement);
    }

    private CancelRequest decodeCancelRequest(final PreparedCommand command) {
        if (!CommandBodies.isRegistryClientBody(command.canonicalBody())) {
            final int expectedGeneration = CommandBodies.decodeDirectCancel(command.canonicalBody());
            return new CancelRequest(expectedGeneration < 0 ? null : (long) expectedGeneration, null);
        }
        final CancelCommandBody body = CommandBodies.decodeCancel(command.canonicalBody());
        requireBodyIdentity(command, body.delayMessageId(), body.retryUntilEpochMs());
        return new CancelRequest(
                body.precondition().expectedGeneration(), body.precondition().expectedStateVersion());
    }

    private RescheduleRequest decodeRescheduleRequest(final PreparedCommand command) {
        if (!CommandBodies.isRegistryClientBody(command.canonicalBody())) {
            final CommandBodies.DirectRescheduleValues values =
                    CommandBodies.decodeDirectReschedule(command.canonicalBody());
            return new RescheduleRequest(
                    values.expectedGeneration() < 0 ? null : (long) values.expectedGeneration(),
                    null,
                    values.deliverAtEpochMs(),
                    values.expireAtEpochMs());
        }
        final RescheduleCommandBody body = CommandBodies.decodeReschedule(command.canonicalBody());
        requireBodyIdentity(command, body.delayMessageId(), body.retryUntilEpochMs());
        return new RescheduleRequest(
                body.precondition().expectedGeneration(),
                body.precondition().expectedStateVersion(),
                body.newDeliverAtEpochMs(),
                body.newExpireAtEpochMs());
    }

    private static void requireBodyIdentity(
            final PreparedCommand command, final DelayMessageId bodyMessageId, final long bodyRetryUntilEpochMs) {
        if (!command.delayMessageId().equals(bodyMessageId) || command.retryUntilEpochMs() != bodyRetryUntilEpochMs) {
            throw new IllegalArgumentException("Client body common fields do not match outer command");
        }
    }

    private static boolean matchesPrecondition(
            final Long expectedGeneration,
            final Long expectedStateVersion,
            final int generation,
            final long stateVersion) {
        return (expectedGeneration == null || expectedGeneration == Integer.toUnsignedLong(generation))
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

    /**
     * Computes the local compatibility identity-freshness boundary. The
     * production Route policy may choose a stricter maximum preparation age;
     * this embedded shard has no separate Route catalog input, so its bounded
     * message-lifetime horizon is the only safe local upper bound.
     */
    private long messageIdentityReuseUntil(final DelayMessageId messageId) {
        Objects.requireNonNull(messageId, "messageId");
        final long freshnessWindow =
                config.maximumPreparationAgeMs() > 0 ? config.maximumPreparationAgeMs() : config.maxMessageLifetimeMs();
        return Math.addExact(messageId.routingId().logicalTimestampEpochMs(), freshnessWindow);
    }

    /**
     * Applies the Route-provided first-seen identity policy when configured.
     * Existing embedded constructors intentionally leave this policy disabled
     * because they have no authenticated Route snapshot; production activation
     * must provide all three strict identity bounds.
     */
    private void validateFirstSeenCommandIdentity(final PreparedCommand command, final SourcePosition sourcePosition) {
        if (config.commandRetryWindowMs() == 0) {
            return;
        }
        final long commandTime = command.commandId().routingId().logicalTimestampEpochMs();
        final long expectedRetryUntil = Math.addExact(commandTime, config.commandRetryWindowMs());
        if (command.retryUntilEpochMs() != expectedRetryUntil) {
            throw new IllegalArgumentException("retryUntil is not bound to command UUIDv7 time");
        }
        validateFirstSeenIdentityTimestamp(commandTime, sourcePosition.brokerPersistenceTimeEpochMs(), "commandId");
    }

    private void validateFirstSeenMessageIdentity(final DelayMessageId messageId, final SourcePosition sourcePosition) {
        if (config.commandRetryWindowMs() == 0) {
            return;
        }
        validateFirstSeenIdentityTimestamp(
                messageId.routingId().logicalTimestampEpochMs(),
                sourcePosition.brokerPersistenceTimeEpochMs(),
                "delayMessageId");
    }

    private void validateFirstSeenIdentityTimestamp(
            final long identityTime, final long brokerPersistenceTime, final String identityName) {
        final long lowerBound = Math.subtractExact(brokerPersistenceTime, config.maximumPreparationAgeMs());
        final long upperBound = Math.addExact(brokerPersistenceTime, config.maximumUuidFutureSkewMs());
        if (identityTime < lowerBound || identityTime > upperBound) {
            throw new IllegalArgumentException(identityName + " UUIDv7 time is outside first-seen Broker window");
        }
    }

    private CommandResult persistRejected(
            final PreparedCommand command, final SourcePosition position, final StableCode code) {
        final CommandResult result = rejected(code, position, -1, 0, null);
        persistResultAndPosition(command, position, result, null);
        return result;
    }

    /** Persists only the position-level fence rejection; never overwrites command identity/result dedupe. */
    private CommandResult persistRejectedPositionOnly(
            final PreparedCommand command, final SourcePosition position, final StableCode code) {
        final CommandResult result = rejected(code, position, -1, 0, null);
        store.write(batch -> {
            batch.putValue(
                    ColumnFamily.DEDUPE,
                    DEDUPE_POSITION_VALUE_TYPE,
                    KeyCodec.dedupePosition(position.canonicalBytes()),
                    command.commandId().bytes());
            persistCommandAppliedStart(batch, position);
            writePosition(batch, position);
        });
        lastAppliedSourcePosition = position;
        mutationSequence = nextMutationSequence();
        return result;
    }

    private CommandResult applied(
            final StableCode code, final SourcePosition sourcePosition, final MessageRecord nextMessage) {
        return new CommandResult(
                ApplyStatus.APPLIED,
                code,
                nextMessage == null ? -1 : nextMessage.generation(),
                nextMessage == null ? 0 : nextMessage.stateVersion(),
                nextMessage == null ? null : nextMessage.status(),
                sourcePosition.canonicalBytes());
    }

    private CommandResult rejected(
            final StableCode code,
            final SourcePosition sourcePosition,
            final int generation,
            final long stateVersion,
            final MessageStatus status) {
        return new CommandResult(
                ApplyStatus.REJECTED, code, generation, stateVersion, status, sourcePosition.canonicalBytes());
    }

    /**
     * Appends the exact command-applied Start to the caller-owned business
     * batch. The objective is immutable catalog input; the Source Position is
     * the sole event identity and Broker-persistence timestamp authority.
     */
    private void persistCommandAppliedStart(final ShardStore.Batch batch, final SourcePosition position)
            throws org.rocksdb.RocksDBException {
        if (commandAppliedSloObjective == null) {
            return;
        }
        final SloSampleStart start = SloAuthoritativeStartFactory.commandApplied(commandAppliedSloObjective, position);
        persistSloStart(batch, start);
    }

    /**
     * Reconstructs the ALL_ACCEPTED due-admission Start from the canonical
     * Admission descriptor. The descriptor is the local semantic evidence
     * projection; production profile/eligibility authority must still prove
     * the same path before this hook is enabled.
     */
    private SloSampleStart dueAdmissionStart(final PublishAdmissionBody body) {
        if (dueAdmissionSloObjective == null) {
            return null;
        }
        final long deliverAt = body.descriptor().deliverAtEpochMs();
        final long actionAt = body.descriptor().actionAtEpochMs();
        final SloPath path = actionAt == deliverAt ? SloPath.ORDINARY_MANAGED : SloPath.MANAGED_PULSAR_HANDOFF;
        return SloAuthoritativeStartFactory.dueAdmission(
                dueAdmissionSloObjective,
                new DelayMessageId(body.messageId()),
                Integer.toUnsignedLong(body.generation()),
                path,
                actionAt,
                Bytes.sha256(body.descriptor().canonicalBytes()));
    }

    private void persistSloStart(final ShardStore.Batch batch, final SloSampleStart start)
            throws org.rocksdb.RocksDBException {
        if (start == null) {
            return;
        }
        if (sloObservationOutboxStore == null) {
            throw new IllegalStateException("SLO Start supplied without an outbox store");
        }
        try {
            sloObservationOutboxStore.reconcileDurableStartsInBatch(batch, List.of(start));
        } catch (SloStartMaterializationException exception) {
            throw exception;
        } catch (IllegalArgumentException | IllegalStateException exception) {
            throw new SloStartMaterializationException("SLO Start materialization failed", exception);
        }
    }

    /** Prevents an SLO evidence-capacity/integrity failure from becoming a stale Admission result. */
    private static final class SloStartMaterializationException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private SloStartMaterializationException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Repairs a missing Start when replay discovers a source turn that was
     * already committed before the SLO objective was activated. This is a
     * deliberate idempotent backfill; new turns use the joint business batch.
     */
    private void repairCommandAppliedStartAfterExistingApply(final SourcePosition position) {
        if (commandAppliedSloObjective != null && sloObservationOutboxStore != null) {
            sloObservationOutboxStore.ensureCommandAppliedStart(commandAppliedSloObjective, position);
        }
    }

    private void persistResultAndPosition(
            final PreparedCommand command,
            final SourcePosition position,
            final CommandResult result,
            final MessageRecord next) {
        final MessageRecord prior = getMessage(command.delayMessageId());
        final boolean existingLane = next != null && readLane(next.laneId()) != null;
        final PayloadReservation reservation = reservationTransition(command, position, result);
        final ShardQuota nextQuota = reservation == null
                ? quotaAfter(prior, next, result, existingLane)
                : quota.removeReservation(reservation.intent().expectedPayloadLength());
        persistMutation(command, position, result, next, reservation, nextQuota);
    }

    private PayloadReservation reservationTransition(
            final PreparedCommand command, final SourcePosition position, final CommandResult result) {
        if (command.type() != com.nereusstream.delay.protocol.CommandType.CANCEL
                || result.stableCode() != StableCode.PAYLOAD_RESERVATION_ABANDONED) {
            return null;
        }
        final PayloadReservation current = findReservationForMessage(command.delayMessageId());
        if (current == null || current.status() != PayloadReservationStatus.RESERVED) {
            return null;
        }
        return current.withLifecycle(
                PayloadReservationStatus.ABANDONED,
                Math.addExact(current.stateVersion(), 1),
                position.canonicalBytes(),
                null);
    }

    private void persistMutation(
            final PreparedCommand command,
            final SourcePosition position,
            final CommandResult result,
            final MessageRecord next,
            final PayloadReservation reservation,
            final ShardQuota nextQuota) {
        final MessageRecord prior = getMessage(command.delayMessageId());
        final MessageRecord persistedNext = normalizeCommandRuntime(command.delayMessageId(), prior, next, result);
        LaneQuotaUsageProjection nextLaneQuota =
                laneQuotaAfterCommand(position, prior, persistedNext, reservation, nextQuota);
        final ScheduleBinding scheduleBinding = scheduleBinding(command, result, persistedNext, reservation);
        final ClaimRecord priorClaim = prior != null && prior.status() == MessageStatus.CLAIMED
                ? findClaimForMessage(command.delayMessageId())
                : null;
        if (prior != null && prior.status() == MessageStatus.CLAIMED && priorClaim == null) {
            throw new IllegalStateException("CLAIMED message has no durable Claim record");
        }
        if (persistedNext != null && priorClaim != null) {
            nextLaneQuota = mutateInflightQuotaUsage(
                    nextLaneQuota,
                    priorClaim.laneId(),
                    priorClaim.laneIncarnation(),
                    claimCharge(priorClaim),
                    false,
                    Math.max(1, nextQuota.usageRevision()));
        }
        final LaneQuotaUsageProjection projectedLaneQuota = nextLaneQuota;
        final Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections = readyProjections(
                position, command.delayMessageId(), prior, persistedNext, reservation, projectedLaneQuota);
        store.write(batch -> {
            if (persistedNext != null) {
                if (prior != null
                        && (prior.status() == MessageStatus.SCHEDULED || prior.status() == MessageStatus.CLAIMED)) {
                    batch.delete(
                            ColumnFamily.TIMELINE,
                            priorClaim == null
                                    ? timelineKey(command.delayMessageId(), prior)
                                    : priorClaim.timelineKey());
                    batch.delete(ColumnFamily.TIMELINE, expiryKey(command.delayMessageId(), prior));
                    if (priorClaim != null) {
                        batch.delete(ColumnFamily.INFLIGHT, priorClaim.encodedKey());
                    }
                    final TerminalGenerationRecord terminal =
                            terminalFor(command, position, result, prior, persistedNext);
                    if (terminal != null) {
                        batch.putValue(
                                ColumnFamily.TERMINAL,
                                1,
                                KeyCodec.terminalGeneration(command.delayMessageId(), terminal.generation()),
                                terminal.encode());
                    }
                }
                batch.putValue(
                        ColumnFamily.ID, 1, KeyCodec.idMessage(command.delayMessageId()), persistedNext.encode());
                if (persistedNext.status() == MessageStatus.SCHEDULED) {
                    batch.putValue(
                            ColumnFamily.TIMELINE,
                            1,
                            timelineKey(command.delayMessageId(), persistedNext),
                            encodeTimelineValue(command.delayMessageId(), persistedNext));
                    batch.putValue(
                            ColumnFamily.TIMELINE,
                            1,
                            expiryKey(command.delayMessageId(), persistedNext),
                            encodeTimelineValue(command.delayMessageId(), persistedNext));
                }
            }
            if (reservation != null) {
                batch.putValue(
                        ColumnFamily.ID, 2, KeyCodec.idReservation(reservation.reservationId()), reservation.encode());
                if (reservation.status() == PayloadReservationStatus.RESERVED) {
                    batch.putValue(
                            ColumnFamily.TIMELINE,
                            5,
                            KeyCodec.reservationExpiry(
                                    reservation.reservationExpiryEpochMs(), reservation.reservationId()),
                            reservation.encode());
                } else {
                    batch.delete(
                            ColumnFamily.TIMELINE,
                            KeyCodec.reservationExpiry(
                                    reservation.reservationExpiryEpochMs(), reservation.reservationId()));
                }
            }
            if (scheduleBinding != null) {
                batch.putValue(
                        ColumnFamily.ID,
                        4,
                        KeyCodec.idScheduleBinding(command.delayMessageId()),
                        scheduleBinding.encode());
            }
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            batch.putValue(
                    ColumnFamily.DEDUPE,
                    1,
                    KeyCodec.dedupeCommand(command.commandId()),
                    new CommandDedupeRecord(command.protocolTuple(), command.commandHash(), result).encode());
            batch.putValue(ColumnFamily.DEDUPE, 2, KeyCodec.dedupeResult(command.commandId()), result.encode());
            batch.putValue(
                    ColumnFamily.DEDUPE,
                    DEDUPE_POSITION_VALUE_TYPE,
                    KeyCodec.dedupePosition(position.canonicalBytes()),
                    command.commandId().bytes());
            persistQuota(batch, nextQuota, projectedLaneQuota);
            persistCommandAppliedStart(batch, position);
            writePosition(batch, position);
        });
        lastAppliedSourcePosition = position;
        mutationSequence = nextMutationSequence();
        quota = nextQuota;
        laneQuotaUsage = projectedLaneQuota;
        lastResolvedSchedule = null;
        lastResolvedScheduleMessageId = null;
        lastResolvedPrepare = null;
    }

    private LaneQuotaUsageProjection laneQuotaAfterCommand(
            final SourcePosition position,
            final MessageRecord prior,
            final MessageRecord next,
            final PayloadReservation reservation,
            final ShardQuota nextQuota) {
        LaneQuotaUsageProjection result = laneQuotaUsage;
        final long revision = Math.max(1, nextQuota.usageRevision());
        boolean reservationHandled = false;
        if (reservation != null) {
            final DestinationLaneId laneId = reservation.intent().laneId();
            final LaneRecord lane = readLane(laneId);
            final byte[] incarnation =
                    lane == null ? LaneRecord.initial(laneId, position).laneIncarnation() : lane.laneIncarnation();
            final long payloadBytes = reservation.intent().expectedPayloadLength();
            final boolean newLane = lane == null;
            switch (reservation.status()) {
                case RESERVED -> result = result.addReservation(laneId, incarnation, payloadBytes, newLane, revision);
                case COMMITTED -> result = result.commitReservation(laneId, incarnation, payloadBytes, revision);
                case ABANDONED, EXPIRED ->
                    result = result.removeReservation(laneId, incarnation, payloadBytes, revision);
            }
            reservationHandled = true;
        }
        if (prior == null && next != null && next.status() == MessageStatus.SCHEDULED && !reservationHandled) {
            final LaneRecord lane = readLane(next.laneId());
            final byte[] incarnation = lane == null
                    ? LaneRecord.initial(next.laneId(), position).laneIncarnation()
                    : lane.laneIncarnation();
            result = result.addSchedule(next.laneId(), incarnation, next.payloadLength(), lane == null, revision);
        } else if (prior != null
                && (prior.status() == MessageStatus.SCHEDULED || prior.status() == MessageStatus.CLAIMED)
                && next != null
                && next.status() == MessageStatus.CANCELED) {
            final LaneRecord lane = readLane(prior.laneId());
            if (lane == null) {
                throw new IllegalStateException("canceled Message references a missing Lane");
            }
            result = result.removeSchedule(prior.laneId(), lane.laneIncarnation(), prior.payloadLength(), revision);
        }
        return result;
    }

    private LaneQuotaUsageProjection removeScheduleQuotaUsage(final MessageRecord message, final ShardQuota nextQuota) {
        final LaneRecord lane = readLane(message.laneId());
        if (lane == null) {
            throw new IllegalStateException("Message references a missing Lane while releasing quota");
        }
        return laneQuotaUsage.removeSchedule(
                message.laneId(),
                lane.laneIncarnation(),
                message.payloadLength(),
                Math.max(1, nextQuota.usageRevision()));
    }

    private LaneQuotaUsageProjection removeReservationQuotaUsage(
            final PayloadReservation reservation, final ShardQuota nextQuota) {
        final LaneRecord lane = readLane(reservation.intent().laneId());
        if (lane == null) {
            throw new IllegalStateException("Reservation references a missing Lane while releasing quota");
        }
        return laneQuotaUsage.removeReservation(
                reservation.intent().laneId(),
                lane.laneIncarnation(),
                reservation.intent().expectedPayloadLength(),
                Math.max(1, nextQuota.usageRevision()));
    }

    private LaneQuotaUsageProjection addClaimQuotaUsage(final ClaimRecord claim) {
        return mutateInflightQuotaUsage(
                laneQuotaUsage,
                claim.laneId(),
                claim.laneIncarnation(),
                claimCharge(claim),
                true,
                Math.max(1, quota.usageRevision()));
    }

    private LaneQuotaUsageProjection removeClaimQuotaUsage(final ClaimRecord claim) {
        return mutateInflightQuotaUsage(
                laneQuotaUsage,
                claim.laneId(),
                claim.laneIncarnation(),
                claimCharge(claim),
                false,
                Math.max(1, quota.usageRevision()));
    }

    private LaneQuotaUsageProjection removeAttemptQuotaUsage(final PublishAttemptLedger ledger) {
        return mutateInflightQuotaUsage(
                laneQuotaUsage,
                ledger.laneId(),
                ledger.laneIncarnation(),
                attemptCharge(ledger),
                false,
                Math.max(1, quota.usageRevision()));
    }

    private LaneQuotaUsageProjection mutateInflightQuotaUsage(
            final LaneQuotaUsageProjection current,
            final DestinationLaneId laneId,
            final byte[] laneIncarnation,
            final PublishAdmissionBody.ChargeVector charge,
            final boolean add,
            final long usageRevision) {
        Objects.requireNonNull(charge, "charge");
        final LaneRecord lane = readLane(laneId);
        if (lane == null) {
            throw new IllegalStateException("inflight quota references a missing Lane");
        }
        final byte[] effectiveIncarnation = lane.laneIncarnation();
        final PublishAdmissionBody.ChargeVector normalized = normalizeInflightCharge(charge);
        if (add) {
            return current.addInflight(
                    laneId,
                    effectiveIncarnation,
                    normalized.inflightMessages(),
                    normalized.inflightBytes(),
                    usageRevision);
        }
        try {
            return current.removeInflight(
                    laneId,
                    effectiveIncarnation,
                    normalized.inflightMessages(),
                    normalized.inflightBytes(),
                    usageRevision);
        } catch (IllegalStateException exception) {
            if (!"missing per-Lane quota entry".equals(exception.getMessage())
                    && !"per-Lane quota usage underflow".equals(exception.getMessage())) {
                throw exception;
            }
            // Direct restore/compatibility fixtures may contain a durable
            // Claim or attempt that predates the persisted class-3 map. Use
            // the ledgers as the source of truth, repair the projection, and
            // then apply the same release atomically in the caller's batch.
            final LaneQuotaUsageProjection rebuilt = rebuildLaneQuotaUsage();
            return rebuilt.removeInflight(
                    laneId,
                    effectiveIncarnation,
                    normalized.inflightMessages(),
                    normalized.inflightBytes(),
                    usageRevision);
        }
    }

    /** Registry field 7 counts the durable Claim/attempt record itself, even for legacy zero-charge adapters. */
    private static PublishAdmissionBody.ChargeVector normalizeInflightCharge(
            final PublishAdmissionBody.ChargeVector charge) {
        return charge.inflightMessages() >= 1
                ? charge
                : new PublishAdmissionBody.ChargeVector(
                        charge.activeMessages(),
                        charge.pendingPayloadBytes(),
                        charge.logicalStateBytes(),
                        charge.retainedBytes(),
                        charge.reservationMessages(),
                        charge.reservationPayloadBytes(),
                        1,
                        charge.inflightBytes(),
                        charge.resultRecords(),
                        charge.resultBytes(),
                        charge.systemMutationRecords(),
                        charge.systemMutationBytes(),
                        charge.outcomeWalBytes(),
                        charge.evidenceRecords(),
                        charge.evidenceBytes(),
                        charge.laneCount(),
                        charge.strongLaneCount());
    }

    private static PublishAdmissionBody.ChargeVector claimCharge(final ClaimRecord claim) {
        return PublishAdmissionBody.ChargeVector.decodeCanonical(
                ClaimResultBody.decodePrecondition(claim.preconditionBytes()).claimedCharge());
    }

    private static PublishAdmissionBody.ChargeVector attemptCharge(final PublishAttemptLedger ledger) {
        try {
            return PublishAdmissionBody.decode(ledger.admissionBytes()).chargeVector();
        } catch (RuntimeException legacyOrMalformedDirectLedger) {
            failClosedForMalformedCanonicalAdmission(ledger.admissionBytes(), legacyOrMalformedDirectLedger);
            return zeroChargeVector();
        }
    }

    private static PublishAdmissionBody.ChargeVector zeroChargeVector() {
        return new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    private void persistQuota(
            final ShardStore.Batch batch, final ShardQuota nextQuota, final LaneQuotaUsageProjection nextLaneQuota)
            throws org.rocksdb.RocksDBException {
        // See persistOutcomeReserve: class 1 is a one-way compatibility
        // projection, never a new write.
        batch.delete(ColumnFamily.META, KeyCodec.metaQuota(META_LEGACY_QUOTA_USAGE));
        batch.putValue(
                ColumnFamily.META,
                7,
                KeyCodec.metaQuota(META_QUOTA_AGGREGATE_USAGE),
                aggregateQuotaUsage(nextLaneQuota, outcomeReserveVector).canonicalBytes());
        if (!Arrays.equals(nextLaneQuota.canonicalBytes(), laneQuotaUsage.canonicalBytes())) {
            batch.putValue(
                    ColumnFamily.META, 7, KeyCodec.metaQuota(META_LANE_QUOTA_USAGE), nextLaneQuota.canonicalBytes());
        }
    }

    private ScheduleBinding scheduleBinding(
            final PreparedCommand command,
            final CommandResult result,
            final MessageRecord next,
            final PayloadReservation reservation) {
        if (result.applyStatus() != ApplyStatus.APPLIED
                || !CommandBodies.isRegistryClientBody(command.canonicalBody())) {
            return null;
        }
        if (command.type() == com.nereusstream.delay.protocol.CommandType.SCHEDULE
                && result.stableCode() == StableCode.SCHEDULED
                && next != null) {
            final ScheduleResolver.ResolvedSchedule resolved =
                    Objects.requireNonNull(lastResolvedSchedule, "resolved Schedule projection");
            if (!resolved.laneId().equals(next.laneId())) {
                throw new IllegalStateException("resolved Schedule Lane changed during apply");
            }
            return ScheduleBinding.fromCommand(command, next.laneId(), resolved.canonicalLaneTuple());
        }
        if (command.type() == com.nereusstream.delay.protocol.CommandType.PREPARE_LARGE_SCHEDULE
                && result.stableCode() == StableCode.OK
                && reservation != null) {
            final ScheduleResolver.ResolvedPrepare resolved =
                    Objects.requireNonNull(lastResolvedPrepare, "resolved Prepare projection");
            if (!resolved.laneId().equals(reservation.intent().laneId())) {
                throw new IllegalStateException("resolved Prepare Lane changed during apply");
            }
            return ScheduleBinding.fromCommand(command, reservation.intent().laneId(), resolved.canonicalLaneTuple());
        }
        return null;
    }

    private Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> readyProjections(
            final SourcePosition position,
            final DelayMessageId messageId,
            final MessageRecord prior,
            final MessageRecord next,
            final PayloadReservation reservation) {
        return readyProjections(position, messageId, prior, next, reservation, Map.of(), laneQuotaUsage);
    }

    private Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> readyProjections(
            final SourcePosition position,
            final DelayMessageId messageId,
            final MessageRecord prior,
            final MessageRecord next,
            final PayloadReservation reservation,
            final LaneQuotaUsageProjection projectedLaneQuota) {
        return readyProjections(position, messageId, prior, next, reservation, Map.of(), projectedLaneQuota);
    }

    private Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> readyProjections(
            final SourcePosition position,
            final DelayMessageId messageId,
            final MessageRecord prior,
            final MessageRecord next,
            final PayloadReservation reservation,
            final Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneRecord> laneOverrides) {
        return readyProjections(position, messageId, prior, next, reservation, laneOverrides, laneQuotaUsage);
    }

    private Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> readyProjections(
            final SourcePosition position,
            final DelayMessageId messageId,
            final MessageRecord prior,
            final MessageRecord next,
            final PayloadReservation reservation,
            final Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneRecord> laneOverrides,
            final LaneQuotaUsageProjection projectedLaneQuota) {
        final Set<com.nereusstream.delay.protocol.DestinationLaneId> laneIds = new HashSet<>();
        if (prior != null) {
            laneIds.add(prior.laneId());
        }
        if (next != null) {
            laneIds.add(next.laneId());
        }
        if (reservation != null) {
            laneIds.add(reservation.intent().laneId());
        }
        final Map<com.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> result = new HashMap<>();
        for (var laneId : laneIds) {
            final LaneRecord previous = readLane(laneId);
            final LaneRecord base = laneOverrides.getOrDefault(
                    laneId, previous == null ? LaneRecord.initial(laneId, position) : previous);
            final int excludedGeneration = prior != null
                            && (prior.status() == MessageStatus.SCHEDULED || prior.status() == MessageStatus.CLAIMED)
                    ? prior.generation()
                    : -1;
            final TimelineCandidate candidate = findLaneCandidate(
                    laneId,
                    messageId,
                    excludedGeneration,
                    next != null && next.status() == MessageStatus.SCHEDULED ? messageId : null,
                    next);
            result.put(laneId, projectLane(laneId, previous, base, candidate, projectedLaneQuota));
        }
        return result;
    }

    private LaneProjection projectLane(
            final com.nereusstream.delay.protocol.DestinationLaneId laneId,
            final LaneRecord previous,
            final LaneRecord base,
            final TimelineCandidate candidate) {
        return projectLane(laneId, previous, base, candidate, laneQuotaUsage);
    }

    private LaneProjection projectLane(
            final com.nereusstream.delay.protocol.DestinationLaneId laneId,
            final LaneRecord previous,
            final LaneRecord base,
            final TimelineCandidate candidate,
            final LaneQuotaUsageProjection projectedLaneQuota) {
        final long nextEligibleAt = candidate == null ? 0 : candidate.nextEligibleAtEpochMs();
        final LaneRecord projected =
                base.nextEligibleAtEpochMs() == nextEligibleAt ? base : base.withNextEligibleAt(nextEligibleAt);
        final ReadyIndexValue ready = projected.schedulable() && candidate != null
                ? new ReadyIndexValue(
                        laneId,
                        candidate.nextEligibleAtEpochMs(),
                        projected.laneVersion(),
                        candidate.messageId(),
                        candidate.generation(),
                        Bytes.sha256(candidate.timelineKey()))
                : null;
        final Long earliestActionAt = candidate == null ? null : candidate.actionAtEpochMs();
        final Long projectedNextEligibleAt = candidate == null ? null : candidate.nextEligibleAtEpochMs();
        final LaneValue previousValue = readLaneValue(laneId);
        final PublishAdmissionBody.ChargeVector laneUsage = previousValue == null || !previousValue.isActive()
                ? projectedLaneQuota.usageFor(laneId, projected.laneIncarnation())
                : projectedLaneQuota.usageFor(
                        laneId, previousValue.asLaneRecord().laneIncarnation());
        return new LaneProjection(
                previous, projected, ready, previousValue, laneUsage, earliestActionAt, projectedNextEligibleAt);
    }

    private void deleteReadyKey(final ShardStore.Batch batch, final LaneRecord lane)
            throws org.rocksdb.RocksDBException {
        if (lane != null && lane.schedulable()) {
            batch.delete(
                    ColumnFamily.TIMELINE,
                    KeyCodec.timelineReady(lane.nextEligibleAtEpochMs(), lane.laneId(), lane.laneVersion()));
        }
    }

    private void putReadyProjection(final ShardStore.Batch batch, final LaneProjection projection)
            throws org.rocksdb.RocksDBException {
        putReadyProjection(batch, projection, null);
    }

    private void putReadyProjection(
            final ShardStore.Batch batch, final LaneProjection projection, final ReadyCertificate activationCertificate)
            throws org.rocksdb.RocksDBException {
        final LaneValue previousValue = projection.previousValue();
        final byte[] laneValue;
        if (previousValue != null && previousValue.typedActiveState() != null) {
            final ActiveLaneState state = previousValue.typedActiveState();
            final byte[] readyKey = projection.readyValue() == null
                    ? null
                    : KeyCodec.timelineReady(
                            projection.readyValue().nextEligibleAtEpochMs(),
                            projection.readyValue().laneId(),
                            projection.readyValue().laneVersion());
            final PublishAdmissionBody.ChargeVector usage = projection.laneUsage();
            final ActiveLaneState nextState = state.withLocalProjection(
                    projection.lane().admissionGate(),
                    projection.lane().runtimeReadiness(),
                    projection.lane().runtimeReadiness() == RuntimeReadiness.BLOCKED
                            ? state.runtimeBlockReason()
                            : null,
                    projection.lane().laneControlVersion(),
                    projection.lane().laneVersion(),
                    projection.lane().weight(),
                    usage,
                    projection.earliestActionAtEpochMs(),
                    projection.nextEligibleAtEpochMs(),
                    readyKey,
                    activationCertificate == null
                            ? (projection.lane().runtimeReadiness() == RuntimeReadiness.READY
                                    ? state.readyCertificate()
                                    : null)
                            : activationCertificate.canonicalBytes());
            laneValue = LaneRecordEnvelope.active(nextState).canonicalBytes();
        } else {
            final ActiveLaneState typed = typedInitialLaneState(projection);
            laneValue = typed == null
                    ? LaneRecordEnvelope.active(projection.lane().encode()).canonicalBytes()
                    : LaneRecordEnvelope.active(typed).canonicalBytes();
        }
        batch.putValue(ColumnFamily.META, 2, KeyCodec.metaLane(projection.lane().laneId()), laneValue);
        if (projection.readyValue() != null) {
            final ReadyIndexValue ready = projection.readyValue();
            batch.putValue(
                    ColumnFamily.TIMELINE,
                    3,
                    KeyCodec.timelineReady(ready.nextEligibleAtEpochMs(), ready.laneId(), ready.laneVersion()),
                    ready.encode());
        }
    }

    /**
     * Creates the first typed Lane projection from the exact resolver
     * tuple. Legacy commands and existing legacy Lanes retain their old
     * compatibility value; they are never upgraded from an arbitrary byte
     * string or from caller-supplied Profile names.
     */
    private ActiveLaneState typedInitialLaneState(final LaneProjection projection) {
        if (projection.previousValue() != null
                || (projection.lane().laneVersion() != 0 && projection.lane().laneVersion() != 1)
                || projection.lane().laneControlVersion() != 1) {
            return null;
        }
        final byte[] tupleBytes;
        if (lastResolvedSchedule != null
                && lastResolvedSchedule.laneId().equals(projection.lane().laneId())) {
            tupleBytes = lastResolvedSchedule.canonicalLaneTuple();
        } else if (lastResolvedPrepare != null
                && lastResolvedPrepare.laneId().equals(projection.lane().laneId())) {
            tupleBytes = lastResolvedPrepare.canonicalLaneTuple();
        } else {
            return null;
        }
        final CanonicalLaneTuple.Projection tuple;
        try {
            tuple = CanonicalLaneTuple.project(tupleBytes);
        } catch (IllegalArgumentException malformedTuple) {
            // The raw resolver remains a compatibility seam for historical
            // tests/clients. A malformed tuple must retain the legacy
            // projection, which strict activation will refuse later, rather
            // than manufacturing typed identity from unparsed bytes.
            return null;
        }
        if (!projection.lane().laneId().equals(DestinationLaneId.derive(tupleBytes))) {
            throw new IllegalStateException("initial typed Lane tuple identity changed during projection");
        }
        final long typedLaneVersion = Math.max(1, projection.lane().laneVersion());
        final Long earliestActionAt = projection.earliestActionAtEpochMs();
        final Long nextEligibleAt = projection.nextEligibleAtEpochMs();
        return new ActiveLaneState(
                projection.lane().laneId(),
                projection.lane().laneIncarnation(),
                projection.lane().admissionGate(),
                projection.lane().runtimeReadiness(),
                null,
                projection.lane().laneControlVersion(),
                typedLaneVersion,
                tuple.destinationProfile(),
                tuple.capabilityProfile(),
                tupleBytes,
                projection.lane().weight(),
                projection.laneUsage(),
                earliestActionAt,
                nextEligibleAt,
                LaneCircuitState.CLOSED,
                0,
                0,
                0,
                0,
                null,
                null,
                null);
    }

    private TimelineCandidate findLaneCandidate(
            final com.nereusstream.delay.protocol.DestinationLaneId laneId,
            final DelayMessageId excludedMessageId,
            final int excludedGeneration,
            final DelayMessageId includedMessageId,
            final MessageRecord includedMessage) {
        TimelineCandidate selected = null;
        if (includedMessage != null
                && includedMessage.status() == MessageStatus.SCHEDULED
                && includedMessageId != null
                && includedMessage.laneId().equals(laneId)) {
            selected = new TimelineCandidate(
                    includedMessageId,
                    includedMessage.generation(),
                    timelineEligibilityAt(includedMessageId, includedMessage),
                    headEligibilityAt(includedMessageId, includedMessage),
                    actionAtFor(includedMessageId, includedMessage),
                    timelineKey(includedMessageId, includedMessage),
                    includedMessage.orderingMode() == com.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO);
        }
        final int candidateLimit = boundedLimitPlusOne(config.maxPendingMessages());
        for (byte tag = 1; tag <= 2; tag++) {
            final byte[] prefix = Bytes.concat(new byte[] {tag, 1}, laneId.bytes());
            final List<com.nereusstream.delay.store.ShardStore.KeyValue> entries =
                    store.scan(ColumnFamily.TIMELINE, prefix, prefixUpperBound(prefix), candidateLimit);
            if (entries.size() >= candidateLimit && config.maxPendingMessages() < Integer.MAX_VALUE) {
                throw new IllegalStateException("timeline candidate scan exceeded configured bound");
            }
            for (var entry : entries) {
                final TimelineCandidate candidate = decodeTimelineCandidate(entry, tag, laneId);
                if (excludedMessageId != null
                        && candidate.messageId().equals(excludedMessageId)
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
            final com.nereusstream.delay.store.ShardStore.KeyValue entry,
            final byte tag,
            final com.nereusstream.delay.protocol.DestinationLaneId expectedLane) {
        final byte[] key = entry.key();
        final int tokenOffset = 2 + 32 + 8;
        final int tokenLength = key.length > tokenOffset && key[tokenOffset] == 1
                ? 9
                : key.length > tokenOffset && key[tokenOffset] == 2 ? 21 : -1;
        if (tokenLength < 0
                || key.length != tokenOffset + tokenLength + DelayMessageId.LENGTH + 4
                || key[0] != tag
                || key[1] != 1) {
            throw new IllegalStateException("invalid timeline key for READY projection");
        }
        final ByteBuffer input = ByteBuffer.wrap(key);
        input.position(2);
        final byte[] laneBytes = new byte[32];
        input.get(laneBytes);
        final com.nereusstream.delay.protocol.DestinationLaneId lane =
                new com.nereusstream.delay.protocol.DestinationLaneId(laneBytes);
        if (!lane.equals(expectedLane)) {
            throw new IllegalStateException("timeline lane prefix mismatch");
        }
        final long eligibleAt = input.getLong();
        input.position(input.position() + tokenLength);
        final byte[] messageBytes = new byte[DelayMessageId.LENGTH];
        input.get(messageBytes);
        final int generation = input.getInt();
        final DelayMessageId messageId = new DelayMessageId(messageBytes);
        final MessageRecord message = getMessage(messageId);
        if (message == null
                || message.status() != MessageStatus.SCHEDULED
                || message.generation() != generation
                || !message.laneId().equals(expectedLane)) {
            throw new IllegalStateException("timeline points to a non-current scheduled message");
        }
        if (!Arrays.equals(key, timelineKey(messageId, message))) {
            throw new IllegalStateException("timeline key does not match the current scheduled message");
        }
        validateTimelineValue(
                com.nereusstream.delay.store.ValueEnvelope.decode(entry.value(), 1)
                        .payload(),
                messageId,
                message,
                key,
                "READY rebuild");
        final boolean ordered =
                message.orderingMode() == com.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO;
        if ((tag == 2) != ordered) {
            throw new IllegalStateException("timeline namespace does not match ordering mode");
        }
        return new TimelineCandidate(
                messageId,
                generation,
                eligibleAt,
                headEligibilityAt(messageId, message),
                actionAtFor(messageId, message),
                key,
                ordered);
    }

    private long headEligibilityAt(final DelayMessageId messageId, final MessageRecord message) {
        final long actionAt = actionAtFor(messageId, message);
        return Math.max(actionAt, message.retryEligibilityAtEpochMs());
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

    /**
     * Bounds scans containing only Claim/Attempt records by both local
     * message capacity and the outcome-reserve record envelope. A single
     * Message may retain several unresolved Attempt ledgers across retry
     * generations, so maxPendingMessages alone is not a safe Attempt bound.
     */
    private long configuredInflightLedgerLimit() {
        return Math.max(config.maxPendingMessages(), config.maxOutcomeReserveRecords());
    }

    /** Bounds a scan that includes both the one-per-Message Claim and open Attempts. */
    private long configuredAllInflightLedgerLimit() {
        try {
            return Math.addExact(config.maxPendingMessages(), config.maxOutcomeReserveRecords());
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private ShardQuota quotaAfter(
            final MessageRecord prior,
            final MessageRecord next,
            final CommandResult result,
            final boolean existingLane) {
        if (prior == null && next != null && result.stableCode() == StableCode.SCHEDULED) {
            return quota.addSchedule(next.payloadLength(), !existingLane);
        }
        if (prior != null
                && (prior.status() == MessageStatus.SCHEDULED || prior.status() == MessageStatus.CLAIMED)
                && next != null
                && next.status() == MessageStatus.CANCELED) {
            return quota.removeSchedule(prior.payloadLength());
        }
        return quota;
    }

    private void persistCommandOnly(final PreparedCommand command, final SourcePosition position) {
        persistPositionOnly(command, position);
    }

    private MessageRecord nextMessage(
            final PreparedCommand command, final SourcePosition position, final CommandResult result) {
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
                yield MessageRecord.current(
                        MessageStatus.SCHEDULED,
                        0,
                        1,
                        intent.deliverAtEpochMs(),
                        intent.expireAtEpochMs(),
                        intent.nativeDeliveryPolicy() == NativeDeliveryPolicy.FORBID
                                ? intent.deliverAtEpochMs()
                                : intent.actionAtEpochMs(),
                        intent.laneId(),
                        intent.orderingMode(),
                        intent.nativeDeliveryPolicy(),
                        intent.payload(),
                        position.canonicalBytes(),
                        intent.payloadReference(),
                        intent.actionAtEpochMs());
            }
            case CANCEL ->
                result.stableCode() == StableCode.CANCELED && prior != null
                        ? MessageRecord.current(
                                MessageStatus.CANCELED,
                                prior.generation(),
                                Math.incrementExact(prior.stateVersion()),
                                prior.deliverAtEpochMs(),
                                prior.expireAtEpochMs(),
                                prior.laneId(),
                                prior.orderingMode(),
                                prior.payload(),
                                prior.scheduleSourcePosition(),
                                prior.payloadReference(),
                                prior.retryEligibilityAtEpochMs())
                        : null;
            case RESCHEDULE ->
                result.stableCode() == StableCode.SUPERSEDED && prior != null
                        ? rescheduledMessage(command, position, prior)
                        : null;
            case PREPARE_LARGE_SCHEDULE, COMMIT_LARGE_SCHEDULE -> null;
        };
    }

    private TerminalGenerationRecord terminalFor(
            final PreparedCommand command,
            final SourcePosition position,
            final CommandResult result,
            final MessageRecord prior,
            final MessageRecord next) {
        final MessageStatus status;
        if (result.stableCode() == StableCode.CANCELED) {
            status = MessageStatus.CANCELED;
        } else if (result.stableCode() == StableCode.SUPERSEDED) {
            status = MessageStatus.SUPERSEDED;
        } else {
            return null;
        }
        return new TerminalGenerationRecord(
                command.delayMessageId(),
                prior.generation(),
                status,
                result.stableCode(),
                next.stateVersion(),
                position.canonicalBytes(),
                prior.runtimeIndex().possibleDestinationDuplicate(),
                prior.runtimeIndex().attemptObligations());
    }

    private MessageRecord rescheduledMessage(
            final PreparedCommand command, final SourcePosition position, final MessageRecord prior) {
        final RescheduleRequest values = decodeRescheduleRequest(command);
        // applyReschedule() already validates the new window and derives the
        // immutable action boundary from the prior runtime projection (or the
        // pinned Profile binding). Rebuild the same value here instead of
        // using the legacy constructor whose default actionAt is deliverAt;
        // otherwise the later persistMutation() normalization would silently
        // erase a certified early handoff on a same-deliverAt Reschedule.
        final long actionAt = actionAtFor(command.delayMessageId(), prior, values.deliverAtEpochMs());
        return MessageRecord.current(
                MessageStatus.SCHEDULED,
                UnsignedInt32.successor(prior.generation()),
                Math.incrementExact(prior.stateVersion()),
                values.deliverAtEpochMs(),
                values.expireAtEpochMs(),
                prior.laneId(),
                prior.orderingMode(),
                prior.payload(),
                position.canonicalBytes(),
                prior.payloadReference(),
                actionAt);
    }

    private void persistPositionOnly(final PreparedCommand command, final SourcePosition position) {
        store.write(batch -> {
            batch.putValue(
                    ColumnFamily.DEDUPE,
                    DEDUPE_POSITION_VALUE_TYPE,
                    KeyCodec.dedupePosition(position.canonicalBytes()),
                    command.commandId().bytes());
            persistCommandAppliedStart(batch, position);
            writePosition(batch, position);
        });
        lastAppliedSourcePosition = position;
        mutationSequence = nextMutationSequence();
    }

    private boolean commandRetryWindowExpired(final PreparedCommand command, final SourcePosition sourcePosition) {
        return (closedIngressDeadlineThrough >= 0 && command.retryUntilEpochMs() <= closedIngressDeadlineThrough)
                || sourcePosition.brokerPersistenceTimeEpochMs() > command.retryUntilEpochMs();
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

    private CommandId readPositionAuditCommandId(final SourcePosition position) {
        final PositionAudit audit = readPositionAudit(position);
        return audit == null ? null : audit.commandId();
    }

    /**
     * Reads the closed POSITION audit union. A source position can be occupied
     * by either a Client Command or a System Mutation, never both.
     */
    private PositionAudit readPositionAudit(final SourcePosition position) {
        final var value = store.getValue(
                ColumnFamily.DEDUPE, KeyCodec.dedupePosition(position.canonicalBytes()), DEDUPE_POSITION_VALUE_TYPE);
        if (value == null) {
            return null;
        }
        final byte[] payload = value.payload();
        if (payload.length == CommandId.LENGTH) {
            final CommandId commandId = new CommandId(payload);
            requireCommandShard(commandId, "position audit lookup");
            return PositionAudit.command(commandId);
        }
        if (payload.length == SystemMutation.HASH_LENGTH) {
            final SystemMutationResult result = getSystemMutationResult(payload);
            if (result == null) {
                throw new IllegalStateException("System Mutation position audit has no result");
            }
            return PositionAudit.system(result.mutationId());
        }
        throw new IllegalStateException("invalid position audit identity length");
    }

    private record PositionAudit(CommandId commandId, byte[] systemMutationId) {
        private PositionAudit {
            if ((commandId == null) == (systemMutationId == null)) {
                throw new IllegalArgumentException("position audit must identify exactly one record kind");
            }
            systemMutationId = systemMutationId == null ? null : Bytes.copy(systemMutationId);
        }

        private static PositionAudit command(final CommandId commandId) {
            return new PositionAudit(Objects.requireNonNull(commandId, "commandId"), null);
        }

        private static PositionAudit system(final byte[] mutationId) {
            Bytes.requireLength(mutationId, SystemMutation.HASH_LENGTH, "mutationId");
            return new PositionAudit(null, mutationId);
        }

        @Override
        public byte[] systemMutationId() {
            return systemMutationId == null ? null : Bytes.copy(systemMutationId);
        }
    }

    private PayloadReservation findReservationForMessage(final DelayMessageId messageId) {
        final int limit = boundedLimitPlusOne(config.maxPendingMessages());
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> entries =
                store.scan(ColumnFamily.ID, new byte[] {2, 1}, new byte[] {3, 1}, Math.max(1, limit));
        if (entries.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("reservation scan exceeded configured bound");
        }
        PayloadReservation found = null;
        for (var entry : entries) {
            final PayloadReservation reservation =
                    effectiveReservation(decodeReservationEntry(entry, "message reservation lookup"));
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
        return reservation.withLifecycle(
                PayloadReservationStatus.EXPIRED, reservation.stateVersion(), reservation.sourcePosition(), null);
    }

    /**
     * Rebuilds the compatibility runtime's exact per-Lane message,
     * reservation, Claim and publish-attempt dimensions before activation.
     * This also backfills the class-3 projection for databases created before
     * the map was persisted; unaccounted retained/external-adapter dimensions
     * are deliberately left at zero until their durable ledgers are wired into
     * this runtime.
     */
    private static ShardQuota rebuildShardQuota(final LaneQuotaUsageProjection projection, final long usageRevision) {
        Objects.requireNonNull(projection, "projection");
        if (usageRevision < 0) {
            throw new IllegalArgumentException("usageRevision must be non-negative");
        }
        long pendingMessages = 0;
        long pendingBytes = 0;
        long reservationMessages = 0;
        long reservationBytes = 0;
        long laneCount = 0;
        for (final var entry : projection.map().entries()) {
            final PublishAdmissionBody.ChargeVector usage = entry.usage();
            try {
                pendingMessages = Math.addExact(pendingMessages, usage.activeMessages());
                pendingBytes = Math.addExact(pendingBytes, usage.pendingPayloadBytes());
                reservationMessages = Math.addExact(reservationMessages, usage.reservationMessages());
                reservationBytes = Math.addExact(reservationBytes, usage.reservationPayloadBytes());
                laneCount = Math.addExact(laneCount, usage.laneCount());
            } catch (ArithmeticException exception) {
                throw new IllegalStateException("shard quota rebuild overflow", exception);
            }
        }
        return new ShardQuota(
                pendingMessages, pendingBytes, reservationMessages, reservationBytes, laneCount, usageRevision);
    }

    private static boolean sameQuotaUsage(final ShardQuota left, final ShardQuota right) {
        return left.pendingMessages() == right.pendingMessages()
                && left.pendingBytes() == right.pendingBytes()
                && left.reservationMessages() == right.reservationMessages()
                && left.reservationBytes() == right.reservationBytes()
                && left.laneCount() == right.laneCount();
    }

    /** Rebuilds the local outcome-record/byte reserve from every open attempt ledger. */
    private OutcomeReserveUsage rebuildOutcomeReserveUsage() {
        final long configuredLimit = Math.max(config.maxPendingMessages(), config.maxOutcomeReserveRecords());
        final int limit = boundedLimitPlusOne(configuredLimit);
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> attempts = store.scan(
                ColumnFamily.INFLIGHT,
                new byte[] {INFLIGHT_PUBLISHING_KIND, 1},
                new byte[] {(byte) (INFLIGHT_UNCERTAIN_KIND + 1), 1},
                limit);
        if (attempts.size() >= limit && configuredLimit < Integer.MAX_VALUE) {
            throw new IllegalStateException("outcome reserve rebuild exceeded configured bound");
        }
        OutcomeReserveUsage result = OutcomeReserveUsage.empty();
        for (var entry : attempts) {
            final PublishAttemptLedger ledger = decodePublishAttempt(entry);
            try {
                result = result.add(outcomeReserveCharge(ledger));
            } catch (ArithmeticException exception) {
                throw new IllegalStateException("outcome reserve rebuild overflow", exception);
            }
        }
        return result;
    }

    /** Rebuilds the exact 66-dimensional outcome projection from open ledgers. */
    private CapacityVector rebuildOutcomeReserveVector() {
        final long configuredLimit = Math.max(config.maxPendingMessages(), config.maxOutcomeReserveRecords());
        final int limit = boundedLimitPlusOne(configuredLimit);
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> attempts = store.scan(
                ColumnFamily.INFLIGHT,
                new byte[] {INFLIGHT_PUBLISHING_KIND, 1},
                new byte[] {(byte) (INFLIGHT_UNCERTAIN_KIND + 1), 1},
                limit);
        if (attempts.size() >= limit && configuredLimit < Integer.MAX_VALUE) {
            throw new IllegalStateException("outcome reserve vector rebuild exceeded configured bound");
        }
        CapacityVector result = CapacityVector.empty();
        for (var entry : attempts) {
            final PublishAttemptLedger ledger = decodePublishAttempt(entry);
            try {
                result = result.add(outcomeCapacityCharge(ledger));
            } catch (ArithmeticException exception) {
                throw new IllegalStateException("outcome reserve vector rebuild overflow", exception);
            }
        }
        return result;
    }

    private LaneQuotaUsageProjection rebuildLaneQuotaUsage() {
        return rebuildLaneQuotaUsage(quota.usageRevision());
    }

    private LaneQuotaUsageProjection rebuildLaneQuotaUsage(final long usageRevision) {
        final long revision = Math.max(1, usageRevision);
        final long configuredLimit = Math.max(config.maxPendingMessages(), config.maxLanes());
        final int limit = boundedLimitPlusOne(configuredLimit);
        LaneQuotaUsageProjection result = LaneQuotaUsageProjection.empty();
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> laneEntries =
                store.scan(ColumnFamily.META, new byte[] {2, 1}, new byte[] {3, 1}, limit);
        if (laneEntries.size() >= limit && configuredLimit < Integer.MAX_VALUE) {
            throw new IllegalStateException("Lane quota rebuild exceeded configured bound");
        }
        for (var entry : laneEntries) {
            final byte[] key = entry.key();
            if (key.length != 2 + DestinationLaneId.LENGTH || key[0] != 2 || key[1] != 1) {
                throw new IllegalStateException("invalid Lane key during quota rebuild");
            }
            final DestinationLaneId laneId = new DestinationLaneId(Arrays.copyOfRange(key, 2, key.length));
            final LaneValue value =
                    decodeLaneValue(ValueEnvelope.decode(entry.value(), 2).payload());
            if (value.isActive()) {
                final LaneRecord lane = value.asLaneRecord();
                if (!lane.laneId().equals(laneId)) {
                    throw new IllegalStateException("Lane key/value identity mismatch during quota rebuild");
                }
                result = result.ensureLane(laneId, lane.laneIncarnation(), revision);
            }
        }

        final List<com.nereusstream.delay.store.ShardStore.KeyValue> messages =
                store.scan(ColumnFamily.ID, new byte[] {1, 1}, new byte[] {2, 1}, limit);
        if (messages.size() >= limit && configuredLimit < Integer.MAX_VALUE) {
            throw new IllegalStateException("message quota rebuild exceeded configured bound");
        }
        for (var entry : messages) {
            if (isRetiredMessageEntry(entry, "quota rebuild")) {
                continue;
            }
            final MessageRecord message = decodeMessageEntry(entry, "quota rebuild");
            if (isTerminalStatus(message.status())) {
                continue;
            }
            final LaneRecord lane = readLane(message.laneId());
            if (lane == null) {
                throw new IllegalStateException("message references a missing Lane during quota rebuild");
            }
            if (lane.admissionGate() == AdmissionGate.RETIRED) {
                throw new IllegalStateException("message references a retired Lane during quota rebuild");
            }
            if (lane.admissionGate() == AdmissionGate.CLOSED && isUnadmittedGeneration(message)) {
                continue;
            }
            result = result.addSchedule(
                    message.laneId(), lane.laneIncarnation(), message.payloadLength(), false, revision);
        }

        final List<com.nereusstream.delay.store.ShardStore.KeyValue> reservations =
                store.scan(ColumnFamily.ID, new byte[] {2, 1}, new byte[] {3, 1}, limit);
        if (reservations.size() >= limit && configuredLimit < Integer.MAX_VALUE) {
            throw new IllegalStateException("reservation quota rebuild exceeded configured bound");
        }
        for (var entry : reservations) {
            final PayloadReservation reservation = decodeReservationEntry(entry, "quota rebuild");
            if (reservation.status() != PayloadReservationStatus.RESERVED) {
                continue;
            }
            final LaneRecord lane = readLane(reservation.intent().laneId());
            if (lane == null) {
                throw new IllegalStateException("reservation references a missing Lane during quota rebuild");
            }
            if (lane.admissionGate() == AdmissionGate.RETIRED) {
                throw new IllegalStateException("reservation references a retired Lane during quota rebuild");
            }
            if (lane.admissionGate() == AdmissionGate.CLOSED) {
                continue;
            }
            result = result.addReservation(
                    reservation.intent().laneId(),
                    lane.laneIncarnation(),
                    reservation.intent().expectedPayloadLength(),
                    false,
                    revision);
        }

        final long configuredInflightLimit = Math.max(config.maxPendingMessages(), config.maxOutcomeReserveRecords());
        final int inflightLimit = boundedLimitPlusOne(configuredInflightLimit);
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> claims = store.scan(
                ColumnFamily.INFLIGHT,
                new byte[] {INFLIGHT_CLAIMED_KIND, 1},
                new byte[] {INFLIGHT_PUBLISHING_KIND, 1},
                inflightLimit);
        if (claims.size() >= inflightLimit && configuredInflightLimit < Integer.MAX_VALUE) {
            throw new IllegalStateException("Claim quota rebuild exceeded configured bound");
        }
        for (var entry : claims) {
            final ClaimRecord claim = decodeClaim(entry);
            result = mutateInflightQuotaUsage(
                    result, claim.laneId(), claim.laneIncarnation(), claimCharge(claim), true, revision);
        }

        final List<com.nereusstream.delay.store.ShardStore.KeyValue> attempts = store.scan(
                ColumnFamily.INFLIGHT,
                new byte[] {INFLIGHT_PUBLISHING_KIND, 1},
                new byte[] {(byte) (INFLIGHT_UNCERTAIN_KIND + 1), 1},
                inflightLimit);
        if (attempts.size() >= inflightLimit && configuredInflightLimit < Integer.MAX_VALUE) {
            throw new IllegalStateException("publish-attempt quota rebuild exceeded configured bound");
        }
        for (var entry : attempts) {
            final PublishAttemptLedger ledger = decodePublishAttempt(entry);
            result = mutateInflightQuotaUsage(
                    result, ledger.laneId(), ledger.laneIncarnation(), attemptCharge(ledger), true, revision);
        }
        return result;
    }

    private static long maxLaneQuotaRevision(final LaneQuotaUsageProjection projection) {
        long revision = 0;
        for (LaneQuotaUsageEntry entry : projection.map().entries()) {
            revision = Math.max(revision, entry.usageRevision());
        }
        return revision;
    }

    /**
     * Reconciles the persisted runtime locator with every live Claim/attempt
     * ledger before the shard can serve work. A checkpoint that loses one
     * side of this relationship is not safely replayable, so activation fails
     * closed instead of guessing a current obligation.
     */
    private void validateRuntimeObligationIndexes() {
        final int limit = boundedLimitPlusOne(config.maxPendingMessages());
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> messageEntries =
                store.scan(ColumnFamily.ID, new byte[] {1, 1}, new byte[] {2, 1}, limit);
        if (messageEntries.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("message runtime-index scan exceeded configured bound");
        }
        final Map<DelayMessageId, MessageRecord> messages = new HashMap<>();
        for (var entry : messageEntries) {
            if (entry.key().length != 2 + DelayMessageId.LENGTH || entry.key()[0] != 1 || entry.key()[1] != 1) {
                throw new IllegalStateException("invalid MESSAGE key while reconciling runtime indexes");
            }
            if (isRetiredMessageEntry(entry, "runtime-index reconciliation")) {
                continue;
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
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> terminalEntries =
                store.scan(ColumnFamily.TERMINAL, new byte[] {1, 1}, new byte[] {2, 1}, limit);
        if (terminalEntries.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("terminal summary reconciliation scan exceeded configured bound");
        }
        for (var entry : terminalEntries) {
            final byte[] key = entry.key();
            if (key.length != 2 + DelayMessageId.LENGTH + 4 || key[0] != 1 || key[1] != 1) {
                throw new IllegalStateException("invalid terminal summary key while reconciling runtime indexes");
            }
            final byte[] messageBytes = Arrays.copyOfRange(key, 2, 2 + DelayMessageId.LENGTH);
            final int generation =
                    ByteBuffer.wrap(key, 2 + DelayMessageId.LENGTH, 4).getInt();
            final TerminalGenerationRecord summary =
                    TerminalGenerationRecord.decode(com.nereusstream.delay.store.ValueEnvelope.decode(entry.value(), 1)
                            .payload());
            final GenerationIdentity identity = new GenerationIdentity(new DelayMessageId(messageBytes), generation);
            if (!summary.messageId().equals(identity.messageId())
                    || summary.generation() != identity.generation()
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

        final List<com.nereusstream.delay.store.ShardStore.KeyValue> claimEntries = store.scan(
                ColumnFamily.INFLIGHT,
                new byte[] {INFLIGHT_CLAIMED_KIND, 1},
                new byte[] {INFLIGHT_PUBLISHING_KIND, 1},
                limit);
        if (claimEntries.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("Claim reconciliation scan exceeded configured bound");
        }
        final Set<DelayMessageId> claimedMessages = new HashSet<>();
        for (var entry : claimEntries) {
            final ClaimRecord claim = decodeClaim(entry);
            final MessageRecord message = messages.get(claim.delayMessageId());
            if (message == null
                    || message.status() != MessageStatus.CLAIMED
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
            if (isRetiredMessageEntry(entry, "runtime-index reconciliation")) {
                continue;
            }
            final DelayMessageId messageId = new DelayMessageId(Arrays.copyOfRange(entry.key(), 2, entry.key().length));
            final MessageRecord message = messages.get(messageId);
            if (message.runtimeIndex().currentWorkKind() == CurrentSendWorkKind.CLAIMED
                    && !claimedMessages.contains(messageId)) {
                throw new IllegalStateException("CLAIMED runtime index has no live Claim record");
            }
        }

        final long configuredInflightLimit = configuredInflightLedgerLimit();
        final int inflightLimit = boundedLimitPlusOne(configuredInflightLimit);
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> attemptEntries = store.scan(
                ColumnFamily.INFLIGHT, new byte[] {INFLIGHT_PUBLISHING_KIND, 1}, new byte[] {4, 1}, inflightLimit);
        if (attemptEntries.size() >= inflightLimit && configuredInflightLimit < Integer.MAX_VALUE) {
            throw new IllegalStateException("attempt reconciliation scan exceeded configured bound");
        }
        for (var entry : attemptEntries) {
            final PublishAttemptLedger ledger = decodePublishAttempt(entry);
            final MessageRecord message = messages.get(ledger.delayMessageId());
            final boolean inCurrentRuntime = message != null
                    && message.generation() == ledger.generation()
                    && containsObligation(message.runtimeIndex(), ledger.obligationRef());
            final TerminalGenerationRecord summary =
                    terminalSummaries.get(new GenerationIdentity(ledger.delayMessageId(), ledger.generation()));
            final boolean inTerminalSummary = summary != null
                    && summary.openObligations().stream()
                            .anyMatch(obligation -> Arrays.equals(
                                    obligation.canonicalBytes(),
                                    ledger.obligationRef().canonicalBytes()));
            if (!inCurrentRuntime && !inTerminalSummary) {
                throw new IllegalStateException("inflight ledger is not represented by the current runtime index");
            }
        }
    }

    private void validateMessageRuntimeBranches(final DelayMessageId messageId, final MessageRecord message) {
        final GenerationRuntimeIndex index = message.runtimeIndex();
        final RetryPolicySemantic pinnedPolicy = retryPolicyCatalog == null
                ? null
                : retryPolicyFor(messageId, message, SourcePositionCodec.decode(message.scheduleSourcePosition()));
        final int maxPublishAdmissions =
                pinnedPolicy == null ? config.maxPublishAdmissions() : pinnedPolicy.maxPublishAdmissions();
        final int maxUncertainRetries =
                pinnedPolicy == null ? config.maxUncertainRetries() : pinnedPolicy.maxUncertainRetries();
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
        if (summary == null
                || summary.status() != message.status()
                || !summary.openObligations().equals(message.runtimeIndex().attemptObligations())
                || summary.possibleDestinationDuplicate()
                        != message.runtimeIndex().possibleDestinationDuplicate()) {
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
        if (ownerEpoch == 0
                || idLength != PublishAttemptLedger.HASH_LENGTH
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

    private static boolean containsObligation(final GenerationRuntimeIndex index, final AttemptObligationRef expected) {
        return index.attemptObligations().stream()
                .anyMatch(actual -> Arrays.equals(actual.canonicalBytes(), expected.canonicalBytes()));
    }

    private LaneRecord readLane(final com.nereusstream.delay.protocol.DestinationLaneId laneId) {
        final LaneValue value = readLaneValue(laneId);
        if (value == null) {
            return null;
        }
        if (value.isActive()) {
            return value.asLaneRecord();
        }
        final LaneTerminalGuard guard = value.terminalGuard();
        return new LaneRecord(
                guard.laneId(),
                guard.laneIncarnation(),
                guard.laneControlVersion(),
                0,
                AdmissionGate.RETIRED,
                RuntimeReadiness.BLOCKED,
                1,
                0);
    }

    private LaneValue readLaneValue(final com.nereusstream.delay.protocol.DestinationLaneId laneId) {
        final var value = store.getValue(ColumnFamily.META, KeyCodec.metaLane(laneId), 2);
        if (value == null) {
            return null;
        }
        return validateLaneValueIdentity(laneId, decodeLaneValue(value.payload()));
    }

    private LaneValue validateLaneValueIdentity(
            final com.nereusstream.delay.protocol.DestinationLaneId laneId, final LaneValue laneValue) {
        if (laneValue.isActive()) {
            final LaneRecord lane = laneValue.asLaneRecord();
            if (!lane.laneId().equals(laneId)) {
                throw new IllegalStateException("active Lane key/value identity mismatch");
            }
        } else if (!laneValue.terminalGuard().laneId().equals(laneId)) {
            throw new IllegalStateException("terminal Lane key/value identity mismatch");
        } else {
            validateSourcePositionShard(
                    laneValue.terminalGuard().terminalSourcePosition().canonicalBytes(), "terminal Lane guard lookup");
        }
        return laneValue;
    }

    /**
     * Conservative local retirement proof. A lane is retired only when this
     * bounded scan can prove that no current message or reversible attempt
     * still names it. If the configured bound is exceeded we fail closed and
     * require the recovery/GC coordinator to retry after compaction.
     */
    private boolean hasLaneRuntimeWork(final com.nereusstream.delay.protocol.DestinationLaneId laneId) {
        final int limit = boundedLimitPlusOne(config.maxPendingMessages());
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> messages =
                store.scan(ColumnFamily.ID, new byte[] {1, 1}, new byte[] {2, 1}, limit);
        if (messages.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("message scan exceeded configured bound during lane retirement");
        }
        for (var entry : messages) {
            if (entry.key().length != 2 + DelayMessageId.LENGTH || entry.key()[0] != 1 || entry.key()[1] != 1) {
                throw new IllegalStateException("invalid MESSAGE key during lane retirement");
            }
            if (isRetiredMessageEntry(entry, "lane retirement")) {
                continue;
            }
            final MessageRecord message = decodeMessageEntry(entry, "lane retirement");
            if (message.laneId().equals(laneId)) {
                return true;
            }
        }
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> reservations =
                store.scan(ColumnFamily.ID, new byte[] {2, 1}, new byte[] {3, 1}, limit);
        if (reservations.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("reservation scan exceeded configured bound during lane retirement");
        }
        for (var entry : reservations) {
            final PayloadReservation reservation = decodeReservationEntry(entry, "lane retirement");
            if (reservation.intent().laneId().equals(laneId)
                    && (reservation.status() == PayloadReservationStatus.RESERVED
                            || reservation.status() == PayloadReservationStatus.COMMITTED)) {
                return true;
            }
        }
        final long configuredInflightLimit = configuredAllInflightLedgerLimit();
        final int inflightLimit = boundedLimitPlusOne(configuredInflightLimit);
        final List<com.nereusstream.delay.store.ShardStore.KeyValue> attempts =
                store.scan(ColumnFamily.INFLIGHT, new byte[] {1, 1}, new byte[] {4, 1}, inflightLimit);
        if (attempts.size() >= inflightLimit && configuredInflightLimit < Integer.MAX_VALUE) {
            throw new IllegalStateException("inflight scan exceeded configured bound during lane retirement");
        }
        for (var entry : attempts) {
            if (entry.key().length < 2 || entry.key()[1] != 1) {
                throw new IllegalStateException("invalid inflight key during lane retirement");
            }
            final com.nereusstream.delay.protocol.DestinationLaneId candidateLane;
            if (entry.key()[0] == INFLIGHT_CLAIMED_KIND) {
                candidateLane = decodeClaim(entry).laneId();
            } else if (entry.key()[0] == INFLIGHT_PUBLISHING_KIND || entry.key()[0] == INFLIGHT_UNCERTAIN_KIND) {
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
        // Pre-envelope databases used the fixed LaneRecord adapter bytes. A
        // zero first byte is the big-endian version marker, while every new
        // protobuf envelope starts with field 1's tag (0x08). Preserve read
        // compatibility without treating malformed new values as legacy data.
        if (payload.length >= 4 && payload[0] == 0) {
            LaneRecord.decode(payload);
            return LaneValue.active(payload);
        }
        final LaneRecordEnvelope envelope = LaneRecordEnvelope.decode(payload);
        return envelope.isActive()
                ? envelope.typedActiveState()
                        .map(LaneValue::typedActive)
                        .orElseGet(() -> LaneValue.active(envelope.activeStateBytes()))
                : LaneValue.terminal(envelope.terminalGuard());
    }

    private PublishAttemptLedger readPublishAttempt(
            final byte[] publishAttemptId, final long ownerEpoch, final byte recordKind) {
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

    private ClaimRecord decodeClaim(final com.nereusstream.delay.store.ShardStore.KeyValue entry) {
        final byte[] key = entry.key();
        if (key.length != 2 + 8 + 4 + ClaimRecord.HASH_LENGTH || key[0] != INFLIGHT_CLAIMED_KIND || key[1] != 1) {
            throw new IllegalStateException("invalid Claim key");
        }
        final ByteBuffer input = ByteBuffer.wrap(key);
        input.position(2);
        final long ownerEpoch = input.getLong();
        final long idLength = Integer.toUnsignedLong(input.getInt());
        if (ownerEpoch == 0 || idLength != ClaimRecord.HASH_LENGTH) {
            throw new IllegalStateException("invalid Claim key owner/ID length");
        }
        final byte[] claimId = new byte[ClaimRecord.HASH_LENGTH];
        input.get(claimId);
        final ClaimRecord claim = ClaimRecord.decode(
                com.nereusstream.delay.store.ValueEnvelope.decode(entry.value(), ClaimRecord.VALUE_TYPE)
                        .payload());
        validateClaimKey(claim, key, claimId, ownerEpoch);
        return validateClaimShard(claim, "claim scan");
    }

    private ClaimRecord validateClaimShard(final ClaimRecord claim, final String context) {
        if (!store.shardId().equals(claim.delayMessageId().routingId().shardId())) {
            throw new IllegalStateException("Claim message shard mismatch during " + context);
        }
        return claim;
    }

    private static void validateClaimKey(
            final ClaimRecord claim, final byte[] key, final byte[] claimId, final long ownerEpoch) {
        if (!Arrays.equals(key, claim.encodedKey())
                || !Arrays.equals(claim.claimId(), claimId)
                || claim.ownerEpoch() != ownerEpoch) {
            throw new IllegalStateException("Claim key/value identity mismatch");
        }
    }

    private PublishAttemptLedger decodePublishAttempt(final com.nereusstream.delay.store.ShardStore.KeyValue entry) {
        final byte[] key = entry.key();
        if (key.length != 2 + 8 + 4 + PublishAttemptLedger.HASH_LENGTH
                || (key[0] != INFLIGHT_PUBLISHING_KIND && key[0] != INFLIGHT_UNCERTAIN_KIND)
                || key[1] != 1) {
            throw new IllegalStateException("invalid open publish attempt key");
        }
        final ByteBuffer input = ByteBuffer.wrap(key);
        input.position(2);
        final long ownerEpoch = input.getLong();
        if (ownerEpoch == 0) {
            throw new IllegalStateException("invalid open publish attempt owner epoch");
        }
        final long idLength = Integer.toUnsignedLong(input.getInt());
        if (idLength != PublishAttemptLedger.HASH_LENGTH) {
            throw new IllegalStateException("invalid open publish attempt ID length");
        }
        final byte[] attemptId = new byte[PublishAttemptLedger.HASH_LENGTH];
        input.get(attemptId);
        final PublishAttemptLedger ledger = PublishAttemptLedger.decode(
                com.nereusstream.delay.store.ValueEnvelope.decode(entry.value(), PublishAttemptLedger.VALUE_TYPE)
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

    private static void validatePublishAttemptKey(
            final PublishAttemptLedger ledger,
            final byte[] key,
            final byte recordKind,
            final byte[] publishAttemptId,
            final long ownerEpoch) {
        final byte expectedKind =
                ledger.state() == AttemptLedgerState.PUBLISHING ? INFLIGHT_PUBLISHING_KIND : INFLIGHT_UNCERTAIN_KIND;
        if (recordKind != expectedKind
                || !Arrays.equals(key, ledger.encodedKey())
                || ledger.ownerEpoch() != ownerEpoch
                || !Bytes.constantTimeEquals(ledger.publishAttemptId(), publishAttemptId)) {
            throw new IllegalStateException("open publish attempt key/value mismatch");
        }
    }

    private byte[] buildClaimPrecondition(
            final byte[] claimId,
            final DelayMessageId messageId,
            final MessageRecord current,
            final LaneRecord lane,
            final byte[] timelineKey,
            final AuthorIdentity owner,
            final long claimDeadlineEpochMs,
            final byte[] materialization,
            final byte[] claimedCharge,
            final int workKind) {
        final byte[] normalizedMaterialization = materialization == null ? new byte[0] : Bytes.copy(materialization);
        final byte[] normalizedCharge = Bytes.copy(Objects.requireNonNull(claimedCharge, "claimedCharge"));
        final TimelineWorkRef sourceWork = current.runtimeIndex().timeline() != null
                        && Arrays.equals(current.runtimeIndex().timeline().encodedTimelineKey(), timelineKey)
                ? current.runtimeIndex().timeline()
                : timelineRuntimeIndex(
                                messageId,
                                current,
                                workKind == 1 ? TimelineWorkKind.INITIAL_SCHEDULE : TimelineWorkKind.DEFINITIVE_RETRY,
                                Math.addExact(current.runtimeIndex().admissionsUsed(), 1),
                                current.stateVersion(),
                                UncertainRetryAuthority.NONE,
                                null,
                                null,
                                current.runtimeIndex())
                        .timeline();
        final byte[] semanticDigest = sourceWork.semanticWorkDigest();
        final int admissionsUsed = current.runtimeIndex().admissionsUsed();
        final int uncertainRetryAdmissionsUsed = current.runtimeIndex().uncertainRetryAdmissionsUsed();
        final byte[] obligationSetDigest = GenerationRuntimeIndex.obligationSetDigest(
                current.runtimeIndex().attemptObligations());
        final byte[] encoded = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, claimId);
            CanonicalProtobuf.bytes(output, 2, messageId.bytes());
            CanonicalProtobuf.uint32Bits(output, 3, current.generation());
            CanonicalProtobuf.int64(output, 4, current.stateVersion());
            CanonicalProtobuf.bytes(output, 5, current.laneId().bytes());
            CanonicalProtobuf.bytes(output, 6, lane.laneIncarnation());
            CanonicalProtobuf.int64(output, 7, lane.laneControlVersion());
            CanonicalProtobuf.int64(output, 8, lane.laneVersion());
            CanonicalProtobuf.bytes(output, 9, Bytes.sha256(timelineKey));
            if (normalizedMaterialization.length != 0) {
                CanonicalProtobuf.bytes(output, 10, normalizedMaterialization);
                CanonicalProtobuf.bytes(
                        output,
                        11,
                        Bytes.sha256(Bytes.utf8("nereus-delay-claim-materialization\0"), normalizedMaterialization));
            }
            CanonicalProtobuf.bytes(output, 12, normalizedCharge);
            CanonicalProtobuf.int64(output, 13, claimDeadlineEpochMs);
            CanonicalProtobuf.bytes(output, 14, owner.asOwnerIdentity().canonicalBytes());
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
        return timelineKey(messageId, message, actionAtFor(messageId, message));
    }

    private byte[] timelineKey(
            final DelayMessageId messageId, final MessageRecord message, final long actionAtEpochMs) {
        final long eligibleAt =
                message.orderingMode() == com.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO
                        ? message.deliverAtEpochMs()
                        : Math.max(actionAtEpochMs, message.retryEligibilityAtEpochMs());
        final SourcePosition position = SourcePositionCodec.decode(message.scheduleSourcePosition());
        return message.orderingMode() == com.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO
                ? KeyCodec.timelineOrdered(
                        message.laneId(), eligibleAt, position.sourceOrderToken(), messageId, message.generation())
                : KeyCodec.timelineDue(
                        message.laneId(), eligibleAt, position.sourceOrderToken(), messageId, message.generation());
    }

    /**
     * Encodes the Registry TimelineWorkRef as the authoritative DUE/ORDERED
     * value. The older TimelineEntry pointer is accepted only when reading
     * pre-migration local stores; all new writes must carry the complete work
     * projection so actionAt/retry eligibility cannot drift from Message.
     */
    private byte[] encodeTimelineValue(final DelayMessageId messageId, final MessageRecord message) {
        Objects.requireNonNull(messageId, "messageId");
        Objects.requireNonNull(message, "message");
        final TimelineWorkRef work = message.runtimeIndex().timeline();
        if (work == null
                || message.status() != MessageStatus.SCHEDULED
                || !Arrays.equals(work.encodedTimelineKey(), timelineKey(messageId, message))) {
            throw new IllegalStateException("scheduled Message lacks the exact TimelineWorkRef projection");
        }
        return work.canonicalBytes();
    }

    /**
     * Validates a DUE/ORDERED value against the current Message. A initial-format
     * TimelineEntry pointer is a bounded read-only migration seam; a current
     * writer's TimelineWorkRef must byte-match the Message runtime index and
     * the physical timeline key.
     */
    private TimelineWorkRef validateTimelineValue(
            final byte[] encodedValue,
            final DelayMessageId messageId,
            final MessageRecord message,
            final byte[] expectedTimelineKey,
            final String context) {
        return validateTimelineValue(encodedValue, messageId, message, expectedTimelineKey, true, context);
    }

    private TimelineWorkRef validateTimelineValue(
            final byte[] encodedValue,
            final DelayMessageId messageId,
            final MessageRecord message,
            final byte[] expectedTimelineKey,
            final boolean requireCurrentWork,
            final String context) {
        if (isLegacyTimelineEntry(encodedValue)) {
            final TimelineEntry legacy = TimelineEntry.decode(encodedValue);
            if (!legacy.messageId().equals(messageId) || legacy.generation() != message.generation()) {
                throw new IllegalStateException("legacy timeline value identity mismatch during " + context);
            }
            return null;
        }
        final TimelineWorkRef work = TimelineWorkRef.decode(encodedValue);
        if (!Arrays.equals(work.encodedTimelineKey(), expectedTimelineKey)) {
            throw new IllegalStateException("TimelineWorkRef key mismatch during " + context);
        }
        if (requireCurrentWork) {
            final TimelineWorkRef current = message.runtimeIndex().timeline();
            if (current != null && !Arrays.equals(current.canonicalBytes(), work.canonicalBytes())) {
                throw new IllegalStateException("TimelineWorkRef disagrees with Message runtime during " + context);
            }
            if (current == null) {
                // MessageRecord versions predating GenerationRuntimeIndex can
                // still be encountered while a local store is being migrated.
                // The rich timeline value remains authoritative for work
                // fields, but its projection must agree with the scalar
                // schedule fields that are available in that legacy record.
                if (work.retryEligibilityAtEpochMs() != message.retryEligibilityAtEpochMs()
                        || work.orderedHeadBlocking()
                                != (message.orderingMode()
                                        == com.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO)
                        || work.actionAtEpochMs() > message.deliverAtEpochMs()) {
                    throw new IllegalStateException("TimelineWorkRef disagrees with legacy Message during " + context);
                }
            }
        }
        return work;
    }

    private static boolean isLegacyTimelineEntry(final byte[] encodedValue) {
        return encodedValue.length >= Integer.BYTES
                && ByteBuffer.wrap(encodedValue, 0, Integer.BYTES).getInt() == 1;
    }

    private long timelineEligibilityAt(final DelayMessageId messageId, final MessageRecord message) {
        if (message.orderingMode() == com.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO) {
            return message.deliverAtEpochMs();
        }
        return Math.max(actionAtFor(messageId, message), message.retryEligibilityAtEpochMs());
    }

    private byte[] expiryKey(final DelayMessageId messageId, final MessageRecord message) {
        return KeyCodec.timelineExpiry(message.expireAtEpochMs(), message.laneId(), messageId, message.generation());
    }

    private void writePosition(final ShardStore.Batch batch, final SourcePosition position)
            throws org.rocksdb.RocksDBException {
        batch.putValue(
                ColumnFamily.META, 1, KeyCodec.metaFixed(META_APPLIED_SOURCE_POSITION), position.canonicalBytes());
        batch.putValue(
                ColumnFamily.META,
                1,
                KeyCodec.metaFixed(META_MUTATION_SEQUENCE),
                Bytes.u64beBits(nextMutationSequence()));
    }

    /**
     * Computes the next persisted shard mutation sequence without permitting an
     * unsigned-domain wrap. Every source-ordered WriteBatch calls this helper
     * before it can publish a new position; the in-memory counter is advanced
     * only after RocksDB acknowledges that batch.
     */
    private long nextMutationSequence() {
        return nextUnsignedSequence(mutationSequence, "shard mutation sequence");
    }

    private static long nextUnsignedSequence(final long sequence, final String name) {
        if (sequence == -1L) {
            throw new ArithmeticException(name + " exhausted");
        }
        return sequence + 1;
    }

    private static long readUnsignedSequence(final byte[] bytes) {
        if (bytes.length != 8) {
            throw new IllegalStateException("invalid shard mutation sequence");
        }
        return ByteBuffer.wrap(bytes).getLong();
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

    private record LaneValue(
            boolean isActive,
            byte[] activeStateBytes,
            ActiveLaneState typedActiveState,
            LaneTerminalGuard terminalGuard) {
        private LaneValue {
            if (isActive == (terminalGuard != null)) {
                throw new IllegalArgumentException("invalid lane value branch");
            }
            if (isActive) {
                Objects.requireNonNull(activeStateBytes, "activeStateBytes");
                activeStateBytes = Bytes.copy(activeStateBytes);
                if (typedActiveState != null && !Arrays.equals(activeStateBytes, typedActiveState.canonicalBytes())) {
                    throw new IllegalArgumentException("typed Lane state bytes are not canonical");
                }
            } else {
                if (activeStateBytes != null) {
                    throw new IllegalArgumentException("terminal lane cannot carry active bytes");
                }
                if (typedActiveState != null) {
                    throw new IllegalArgumentException("terminal lane cannot carry typed active state");
                }
                Objects.requireNonNull(terminalGuard, "terminalGuard");
            }
        }

        private static LaneValue active(final byte[] stateBytes) {
            return new LaneValue(true, stateBytes, null, null);
        }

        private static LaneValue typedActive(final ActiveLaneState state) {
            Objects.requireNonNull(state, "state");
            return new LaneValue(true, state.canonicalBytes(), state, null);
        }

        private static LaneValue terminal(final LaneTerminalGuard guard) {
            return new LaneValue(false, null, null, guard);
        }

        @Override
        public byte[] activeStateBytes() {
            return activeStateBytes == null ? null : Bytes.copy(activeStateBytes);
        }

        private LaneRecord asLaneRecord() {
            if (!isActive) {
                throw new IllegalStateException("lane value is terminal");
            }
            if (typedActiveState == null) {
                return LaneRecord.decode(activeStateBytes);
            }
            final long weight = typedActiveState.schedulerWeight();
            final long laneVersion = typedActiveState.laneVersion();
            if (weight <= 0 || weight > Integer.MAX_VALUE || laneVersion <= 0) {
                throw new IllegalStateException("typed Lane state exceeds compatibility runtime range");
            }
            final long nextEligible =
                    typedActiveState.nextEligibleAtEpochMs() == null ? 0 : typedActiveState.nextEligibleAtEpochMs();
            return new LaneRecord(
                    typedActiveState.laneId(),
                    typedActiveState.laneIncarnation(),
                    typedActiveState.laneControlVersion(),
                    laneVersion,
                    typedActiveState.admissionGate(),
                    typedActiveState.runtimeReadiness(),
                    Math.toIntExact(weight),
                    nextEligible);
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
        return new ReadyKey(
                new com.nereusstream.delay.protocol.DestinationLaneId(laneBytes), nextEligibleAt, laneVersion);
    }

    private record ReadyKey(
            com.nereusstream.delay.protocol.DestinationLaneId laneId, long nextEligibleAtEpochMs, long laneVersion) {}

    private record TimelineCandidate(
            DelayMessageId messageId,
            int generation,
            long eligibleAtEpochMs,
            long nextEligibleAtEpochMs,
            long actionAtEpochMs,
            byte[] timelineKey,
            boolean ordered)
            implements Comparable<TimelineCandidate> {
        private TimelineCandidate {
            timelineKey = Bytes.copy(timelineKey);
            if (eligibleAtEpochMs < 0 || nextEligibleAtEpochMs < 0 || actionAtEpochMs < 0) {
                throw new IllegalArgumentException("timeline candidate times must be non-negative");
            }
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

    private record LaneProjection(
            LaneRecord previousLane,
            LaneRecord lane,
            ReadyIndexValue readyValue,
            LaneValue previousValue,
            PublishAdmissionBody.ChargeVector laneUsage,
            Long earliestActionAtEpochMs,
            Long nextEligibleAtEpochMs) {}

    private record LaneClaimRollback(ClaimRecord claim, MessageRecord nextMessage) {}

    private record CloseAccounting(
            long pendingMessages, long pendingBytes, long reservationMessages, long reservationBytes) {
        private static CloseAccounting empty() {
            return new CloseAccounting(0, 0, 0, 0);
        }
    }

    private record CursorScan(List<com.nereusstream.delay.store.ShardStore.KeyValue> entries, boolean hasMore) {
        private CursorScan {
            entries = List.copyOf(entries);
        }
    }

    private record ClosedMessageAction(
            DelayMessageId messageId,
            MessageRecord current,
            ClaimRecord claim,
            byte[] timelineKey,
            byte[] expiryKey,
            MessageRecord terminalMessage,
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

    private record ClosedReservationAction(PayloadReservation reservation, PayloadReservation closedReservation) {}

    private record AdmissionReplayState(boolean claimMayBeMissing, boolean uncertainRetryAdmission) {}

    private record GenerationIdentity(DelayMessageId messageId, int generation) {}

    private record CancelRequest(Long expectedGeneration, Long expectedStateVersion) {}

    private record RescheduleRequest(
            Long expectedGeneration, Long expectedStateVersion, long deliverAtEpochMs, long expireAtEpochMs) {}

    private record ScheduleApplication(
            long deliverAtEpochMs,
            long expireAtEpochMs,
            long actionAtEpochMs,
            com.nereusstream.delay.protocol.DestinationLaneId laneId,
            com.nereusstream.delay.protocol.OrderingMode orderingMode,
            byte[] payload,
            PayloadReference payloadReference,
            NativeDeliveryPolicy nativeDeliveryPolicy) {
        private ScheduleApplication {
            Objects.requireNonNull(laneId, "laneId");
            Objects.requireNonNull(orderingMode, "orderingMode");
            Objects.requireNonNull(payload, "payload");
            Objects.requireNonNull(nativeDeliveryPolicy, "nativeDeliveryPolicy");
            if (deliverAtEpochMs < 0
                    || expireAtEpochMs < deliverAtEpochMs
                    || actionAtEpochMs < 0
                    || actionAtEpochMs > deliverAtEpochMs
                    || payloadReference != null && payload.length != 0) {
                throw new IllegalArgumentException("invalid resolved Schedule projection");
            }
            payload = Bytes.copy(payload);
            if (nativeDeliveryPolicy != NativeDeliveryPolicy.FORBID
                    && orderingMode != com.nereusstream.delay.protocol.OrderingMode.BEST_EFFORT) {
                throw new IllegalArgumentException("native Schedule projection requires BEST_EFFORT ordering");
            }
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

    private static int compareGeneration(final int left, final int right) {
        return UnsignedInt32.compare(left, right);
    }

    public record TimelineWork(
            DelayMessageId messageId,
            com.nereusstream.delay.protocol.DestinationLaneId laneId,
            int generation,
            long eligibleAtEpochMs,
            boolean ordered) {
        public TimelineWork {
            Objects.requireNonNull(messageId, "messageId");
            Objects.requireNonNull(laneId, "laneId");
            if (eligibleAtEpochMs < 0) {
                throw new IllegalArgumentException("invalid timeline work");
            }
        }
    }

    public record ReadyWork(
            com.nereusstream.delay.protocol.DestinationLaneId laneId,
            DelayMessageId messageId,
            int generation,
            long nextEligibleAtEpochMs,
            long laneVersion,
            boolean ordered) {
        public ReadyWork {
            Objects.requireNonNull(laneId, "laneId");
            Objects.requireNonNull(messageId, "messageId");
            if (nextEligibleAtEpochMs < 0 || laneVersion < 0) {
                throw new IllegalArgumentException("invalid READY work");
            }
        }
    }

    public record ExpiryWork(
            DelayMessageId messageId,
            com.nereusstream.delay.protocol.DestinationLaneId laneId,
            int generation,
            long expireAtEpochMs) {
        public ExpiryWork {
            Objects.requireNonNull(messageId, "messageId");
            Objects.requireNonNull(laneId, "laneId");
            if (expireAtEpochMs < 0) {
                throw new IllegalArgumentException("invalid expiry work");
            }
        }
    }

    public record ReservationExpiryWork(
            byte[] reservationId, DelayMessageId messageId, long reservationExpiryEpochMs, long stateVersion) {
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

    /** Outcome of one bounded, source-fence-derived reservation materialization. */
    public record ReservationExpiryMaterializationResult(Kind kind, PayloadReservation reservation) {
        public ReservationExpiryMaterializationResult {
            Objects.requireNonNull(kind, "kind");
            if (kind == Kind.NOT_FOUND && reservation != null) {
                throw new IllegalArgumentException("missing reservation cannot carry a projection");
            }
            if (kind != Kind.NOT_FOUND && reservation == null) {
                throw new IllegalArgumentException("materialization outcome requires a reservation projection");
            }
            if (kind == Kind.MATERIALIZED && reservation.status() != PayloadReservationStatus.EXPIRED) {
                throw new IllegalArgumentException("materialized reservation must be EXPIRED");
            }
        }

        public enum Kind {
            MATERIALIZED,
            ALREADY_TERMINAL,
            STALE,
            NOT_FOUND
        }

        private static ReservationExpiryMaterializationResult materialized(final PayloadReservation reservation) {
            return new ReservationExpiryMaterializationResult(Kind.MATERIALIZED, reservation);
        }

        private static ReservationExpiryMaterializationResult alreadyTerminal(final PayloadReservation reservation) {
            return new ReservationExpiryMaterializationResult(Kind.ALREADY_TERMINAL, reservation);
        }

        private static ReservationExpiryMaterializationResult stale(final PayloadReservation reservation) {
            return new ReservationExpiryMaterializationResult(Kind.STALE, reservation);
        }

        private static ReservationExpiryMaterializationResult notFound() {
            return new ReservationExpiryMaterializationResult(Kind.NOT_FOUND, null);
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
            if (closeVersion <= 0
                    || nextEligibleAtEpochMs < 0
                    || !laneId.equals(cursor.laneId())
                    || closeVersion != cursor.closeVersion()) {
                throw new IllegalArgumentException("invalid Lane close materialization work");
            }
        }
    }

    /** Result of one bounded local Lane-close materialization turn. */
    public record LaneCloseMaterializationResult(
            com.nereusstream.delay.protocol.DestinationLaneId laneId,
            long closeVersion,
            int scannedRecords,
            int materializedMessages,
            int materializedReservations,
            boolean complete) {
        public LaneCloseMaterializationResult {
            Objects.requireNonNull(laneId, "laneId");
            final int materializedTotal = checkedMaterializedTotal(materializedMessages, materializedReservations);
            if (closeVersion < 0
                    || scannedRecords < 0
                    || materializedMessages < 0
                    || materializedReservations < 0
                    || materializedTotal > scannedRecords) {
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

    /** Outcome of one exact close-cursor candidate after GC queue wait. */
    public record LaneCloseMaterializationExecutionResult(
            Kind kind, LaneCloseMaterializationResult result, LaneCloseMaterializationCursor currentCursor) {
        public LaneCloseMaterializationExecutionResult {
            Objects.requireNonNull(kind, "kind");
            if (kind == Kind.MATERIALIZED && result == null) {
                throw new IllegalArgumentException("materialized close work requires a result");
            }
            if (kind != Kind.MATERIALIZED && result != null) {
                throw new IllegalArgumentException("non-materialized close work cannot carry a turn result");
            }
            if (kind == Kind.STALE && currentCursor == null) {
                throw new IllegalArgumentException("stale close work requires the current cursor");
            }
            if (kind != Kind.STALE && currentCursor != null) {
                throw new IllegalArgumentException("only stale close work carries the current cursor");
            }
        }

        public enum Kind {
            MATERIALIZED,
            STALE,
            NOT_FOUND
        }

        private static LaneCloseMaterializationExecutionResult materialized(
                final LaneCloseMaterializationResult result) {
            return new LaneCloseMaterializationExecutionResult(
                    Kind.MATERIALIZED, Objects.requireNonNull(result, "result"), null);
        }

        private static LaneCloseMaterializationExecutionResult stale(
                final LaneCloseMaterializationCursor currentCursor) {
            return new LaneCloseMaterializationExecutionResult(
                    Kind.STALE, null, Objects.requireNonNull(currentCursor, "currentCursor"));
        }

        private static LaneCloseMaterializationExecutionResult notFound() {
            return new LaneCloseMaterializationExecutionResult(Kind.NOT_FOUND, null, null);
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
