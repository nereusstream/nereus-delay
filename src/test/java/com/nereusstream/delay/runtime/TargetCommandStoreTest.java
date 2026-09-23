package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.ownership.InMemoryControlTargetRegistrationAuthority;
import com.nereusstream.delay.ownership.InMemoryOwnerLeaseStore;
import com.nereusstream.delay.ownership.OwnerLease;
import com.nereusstream.delay.ownership.OwnerLeaseStore;
import com.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import com.nereusstream.delay.ownership.ShardLifecycleState;
import com.nereusstream.delay.ownership.SourceAcknowledgement;
import com.nereusstream.delay.ownership.SourceApplyCoordinator;
import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.ownership.SourceRecordConsumer;
import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.ownership.SourceReplayRecord;
import com.nereusstream.delay.ownership.SourceReplaySuccessor;
import com.nereusstream.delay.ownership.TargetOwnerDrainCoordinator;
import com.nereusstream.delay.ownership.TargetReservationClosureWorkClassExecutor;
import com.nereusstream.delay.ownership.TargetReservationExpiryWorkClassExecutor;
import com.nereusstream.delay.ownership.TargetReservationGcRuntime;
import com.nereusstream.delay.ownership.TargetReservationQueryWorkClassExecutor;
import com.nereusstream.delay.ownership.TargetSourceApplyRuntime;
import com.nereusstream.delay.ownership.TargetWorkerShardFleetRuntime;
import com.nereusstream.delay.ownership.TargetWorkerShardRuntime;
import com.nereusstream.delay.ownership.WorkerSourceApplyLoop;
import com.nereusstream.delay.protocol.AcknowledgementSet;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CancelCommandBody;
import com.nereusstream.delay.protocol.CanonicalPayloadCommitProof;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.CheckpointUploadState;
import com.nereusstream.delay.protocol.CloseLaneRequest;
import com.nereusstream.delay.protocol.ClosePolicy;
import com.nereusstream.delay.protocol.CommandHash;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.CommandType;
import com.nereusstream.delay.protocol.CommitLargeScheduleBody;
import com.nereusstream.delay.protocol.ControlAuthor;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlReason;
import com.nereusstream.delay.protocol.ControlReasonKind;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.ControlRole;
import com.nereusstream.delay.protocol.ControlRoleSet;
import com.nereusstream.delay.protocol.ControlTargetKind;
import com.nereusstream.delay.protocol.ControlTargetRef;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.MessagePrecondition;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PayloadProofTrustSetControlState;
import com.nereusstream.delay.protocol.PayloadProofTrustSetSemantic;
import com.nereusstream.delay.protocol.PayloadProofVerifierKey;
import com.nereusstream.delay.protocol.PrepareLargeScheduleBody;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.ProfileBindingControlState;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.RescheduleCommandBody;
import com.nereusstream.delay.protocol.ScheduleCommandBody;
import com.nereusstream.delay.protocol.SelfRoutingId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetCloseBody;
import com.nereusstream.delay.protocol.TargetCloseRecord;
import com.nereusstream.delay.protocol.TargetCloseRequest;
import com.nereusstream.delay.protocol.TargetControlScope;
import com.nereusstream.delay.protocol.TargetDispatchCompatibility;
import com.nereusstream.delay.protocol.TargetMembershipGrant;
import com.nereusstream.delay.protocol.TargetNativePolicyScope;
import com.nereusstream.delay.protocol.TargetPartitionHashInput;
import com.nereusstream.delay.protocol.TargetPartitionPolicy;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaGrant;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlBody;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.protocol.TargetTimeFenceBody;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.CheckpointManifestLimits;
import com.nereusstream.delay.store.CheckpointUploadIntentStore;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Actual first Schedule and subsequent Worker transitions; membership/Native/Route/lease authorities are fixtures. */
class TargetCommandStoreTest {
    @TempDir
    Path root;

    @ParameterizedTest
    @CsvSource({"false,false", "true,false", "false,true", "true,true"})
    void modifiesActualTimelineOrClaimWithHistoryAndFirstResults(boolean claimed, boolean rescheduled)
            throws Exception {
        final var initial =
                TargetScheduleBinding.decode(vector("target-binding-channel-vectors.properties", "binding.best"));
        final var template = TargetQuotaGrantActivation.decode(raw("target.initial.activation"));
        final var originalGrant = template.grant();
        final var scope = new TargetQuotaScope(
                initial.bindingSource().shardId(), originalGrant.scope().tenantScope(), null);
        final var firstSource = (KafkaSourcePosition) initial.bindingSource();
        final var origin = source(firstSource, firstSource.offset() - 2, firstSource.brokerLogAppendTimeEpochMs() - 2);
        final long[] amounts = originalGrant.limit().resources().amounts();
        Arrays.fill(amounts, 0, 15, 1L << 30);
        Arrays.fill(amounts, 50, 55, 1L << 30);
        final var request = new TargetQuotaGrantControlRequest(
                new TargetQuotaGrant(
                        scope,
                        bytes(32, 0x41),
                        1,
                        originalGrant.accounting(),
                        new TargetQuotaUsage(new CapacityVector(amounts), 64, 64, 64, 64),
                        originalGrant.tenantPolicyVersion(),
                        originalGrant.tenantPolicyHash()),
                null,
                null);
        final var keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final var actor = new ControlAuthorizationContext(
                bytes(32, 0xa1), ControlRoleSet.of(ControlRole.PLATFORM_OPERATOR), bytes(32, 0xa2));
        final var signed = signed(request, bytes(32, 0x42), actor, keys);
        final var registrations = new InMemoryControlTargetRegistrationAuthority();
        registrations.register(signed.control());
        final var config = ShardStoreConfig.defaults(root);
        final com.nereusstream.delay.protocol.TargetPartitionId[] reopenedCloseTargets =
                new com.nereusstream.delay.protocol.TargetPartitionId[3];
        record ReopenOwner(
                SourceAssignment assignment,
                OxiaOwnerLeaseStore leases,
                long priorEpoch,
                java.util.concurrent.atomic.AtomicBoolean loseTransitionResponse,
                java.util.concurrent.atomic.AtomicBoolean loseReleaseResponse,
                java.util.concurrent.atomic.AtomicBoolean throwTransitionAfterCommit,
                java.util.concurrent.atomic.AtomicBoolean throwReleaseAfterCommit) {}
        final ReopenOwner[] reopenOwner = new ReopenOwner[1];
        final Path physicalDb;
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.openTarget(config, scope.shard(), resources)) {
            physicalDb = store.dbPath();
            final var initialized = TargetStoreBootstrap.commit(
                    TargetStoreBootstrap.prepare(
                            store,
                            scope,
                            bytes(16, 0xcc),
                            new TargetStoreBackend.WriteLimits(64, 2 << 20),
                            budget(),
                            signed.control(),
                            signed.mutation(),
                            origin,
                            authority(registrations, keys, actor, origin, request, (a, b, c, d) -> {}),
                            (a, b, c) -> {}),
                    (a, b, c) -> guard());
            final var backend = initialized.backend();
            final var lineage = initialized.root().recoveryLineage();
            final var model =
                    TargetMessageRecord.decode(vector("target-identity-vectors.properties", "message.initial"));
            final var physical =
                    CanonicalTargetPartition.decode(vector("target-identity-vectors.properties", "pulsar.canonical"));
            reopenedCloseTargets[0] = physical.id();
            final long[] targetAmounts = amounts.clone();
            Arrays.fill(targetAmounts, 50, 55, 0);
            targetAmounts[CapacityDimension.ACTIVE_MESSAGES.wireValue() - 1] = 1;
            targetAmounts[CapacityDimension.RESERVATION_MESSAGES.wireValue() - 1] = 1;
            final var targetRequest = new TargetQuotaGrantControlRequest(
                    new TargetQuotaGrant(
                            scope.forTarget(initial.target()),
                            bytes(32, 0x51),
                            1,
                            originalGrant.accounting(),
                            new TargetQuotaUsage(new CapacityVector(targetAmounts), 1, 64, 64, 64),
                            originalGrant.tenantPolicyVersion(),
                            originalGrant.tenantPolicyHash()),
                    null,
                    null);
            final var grantAt = source(origin, origin.offset() + 1, origin.brokerLogAppendTimeEpochMs() + 1);
            final var targetControl = signed(targetRequest, bytes(32, 0x52), actor, keys);
            registrations.register(targetControl.control());
            final var grantStore = new TargetQuotaGrantStore(backend, scope, lineage, 16, 1);
            grantStore.commit(
                    grantStore.prepareFirst(
                            budget(),
                            targetControl.control(),
                            targetControl.mutation(),
                            grantAt,
                            authority(registrations, keys, actor, grantAt, targetRequest, (a, b, c, d) -> {})),
                    (a, b, c) -> guard());
            final var grant = TargetQuotaGrantActivation.decode(TargetValueEnvelope.decode(
                            store.get(
                                    ColumnFamily.META,
                                    Bytes.concat(
                                            new byte[] {TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG, 1},
                                            targetRequest.next().scope().keySuffix())),
                            TargetQuotaGrantActivation.VALUE_TYPE)
                    .payload());
            final var allocation = grant.allocation();
            final var membershipAt = source(grantAt, grantAt.offset() + 1, grantAt.brokerLogAppendTimeEpochMs() + 1);
            final var scheduleAt =
                    source(membershipAt, membershipAt.offset() + 1, membershipAt.brokerLogAppendTimeEpochMs() + 1);
            final var dispatch = TargetDispatchCompatibility.decode(
                    vector("target-compatibility-vectors.properties", "pulsar.journal.dispatch"));
            final var controlModel =
                    TargetControlScope.decode(vector("target-compatibility-vectors.properties", "scope.shared"));
            final var controls = new TargetControlScope(
                    physical.id(), scope.shard(), controlModel.controls(), controlModel.permits());
            final var capability = new ProfileSemanticEnvelope(
                    ProfileKind.DELIVERY_CAPABILITY, Bytes.utf8("cap"), 1, dispatch.capability());
            final var destination = new ProfileSemanticEnvelope(
                    ProfileKind.DESTINATION,
                    Bytes.utf8("member"),
                    1,
                    new DestinationProfileSemantic(
                            AdapterKind.PULSAR,
                            physical.resource(),
                            8,
                            TargetPartitionPolicy.EXPLICIT_ONLY,
                            TargetPartitionHashInput.DELAY_MESSAGE_ID,
                            List.of(5),
                            capability.ref(),
                            3,
                            60000,
                            bytes(32, 0xaa),
                            20000,
                            10000,
                            10000,
                            1,
                            Bytes.utf8("member"),
                            86400000,
                            172800000,
                            2,
                            bytes(32, 0xbb)));
            final var membership = new TargetMembershipGrant(
                    scope.tenantScope(),
                    destination.ref(),
                    dispatch,
                    dispatch,
                    controls,
                    bytes(32, 0x71),
                    bytes(32, 0x72),
                    bytes(32, 0x73),
                    membershipAt);
            final var nativeModel = TargetNativePolicyScope.decode(TargetValueEnvelope.decode(
                            vector("target-native-policy-vectors.properties", "scope.value"),
                            TargetNativePolicyScope.VALUE_TYPE)
                    .payload());
            final var nativeScope = new TargetNativePolicyScope(
                    nativeModel.authorityNamespace(),
                    nativeModel.controlResourceScope(),
                    scope.shard(),
                    physical.id(),
                    allocation.identity().accountingIncarnation(),
                    initial.domain(),
                    dispatch.digest(),
                    controls.digest(),
                    60000,
                    nativeModel.artifacts());
            // Source-applied membership/Native controls are fixture inputs until their formal handlers are wired.
            new TargetMessageStore(backend, 1, 1, 1)
                    .applyAccounted(
                            budget(),
                            reader -> new TargetMessageStore.Input(
                                    List.of(),
                                    List.of(),
                                    List.of(
                                            reader.replace(
                                                    ColumnFamily.META,
                                                    TargetKeyCodec.identity(physical.id()),
                                                    CanonicalTargetPartition.VALUE_TYPE,
                                                    physical.canonicalBytes()),
                                            reader.replace(
                                                    ColumnFamily.META,
                                                    membership.encodedKey(),
                                                    TargetMembershipGrant.VALUE_TYPE,
                                                    membership.canonicalBytes()),
                                            reader.replace(
                                                    ColumnFamily.META,
                                                    nativeScope.encodedKey(),
                                                    TargetNativePolicyScope.VALUE_TYPE,
                                                    nativeScope.canonicalBytes()))),
                            new TargetSourceAccounting(
                                    scope, lineage, membershipAt, membership.sourceMutationDigest(), 16, 1, 1),
                            (a, b, c) -> guard());
            final var priorIntent =
                    ScheduleCommandBody.decode(initial.canonicalBody()).intent();
            final var intent = CanonicalScheduleIntent.create(
                    destination.ref(),
                    priorIntent.retryPolicy(),
                    scheduleAt.brokerLogAppendTimeEpochMs() + 100,
                    scheduleAt.brokerLogAppendTimeEpochMs() + 2000,
                    priorIntent.deliveryMode(),
                    priorIntent.orderingMode(),
                    priorIntent.orderingKey(),
                    model.inlinePayload(),
                    null,
                    priorIntent.adapterMetadata(),
                    priorIntent.businessKey(),
                    priorIntent.eventTimeEpochMs(),
                    model.nativeDeliveryPolicy());
            final var messageId = new DelayMessageId(
                    cancel(initial.messageId(), scheduleAt, 9).commandId().bytes());
            final var scheduleBody =
                    new ScheduleCommandBody(messageId, scheduleAt.brokerLogAppendTimeEpochMs() + 1000, intent);
            final var scheduleId = cancel(messageId, scheduleAt, 10).commandId();
            final var schedule = new PreparedCommand(
                    scope.shard(),
                    scheduleId,
                    messageId,
                    CommandType.SCHEDULE,
                    ProtocolTuple.managedCommand(),
                    scheduleBody.retryUntilEpochMs(),
                    scheduleBody.canonicalBytes(),
                    CommandHash.compute(
                            ProtocolTuple.managedCommand(),
                            CommandType.SCHEDULE,
                            scheduleId,
                            messageId,
                            scheduleBody.retryUntilEpochMs(),
                            scheduleBody.canonicalBytes()));
            final var binding = new TargetScheduleBinding(
                    messageId,
                    CommandType.SCHEDULE,
                    scheduleBody.canonicalBytes(),
                    scheduleAt,
                    physical.id(),
                    initial.domain(),
                    allocation.identity().accountingIncarnation(),
                    dispatch.digest(),
                    dispatch.digest(),
                    controls.digest(),
                    membership.digest(),
                    nativeScope.digest(),
                    null);
            final var profiles = ProfileBindingControlState.empty()
                    .activate(destination.ref(), origin)
                    .activate(capability.ref(), grantAt);
            final var scheduleAuthority = new TargetScheduleRegistration.Authority(
                    binding,
                    physical,
                    destination,
                    capability,
                    profiles,
                    ref -> new TargetMembershipAuthority.AppliedGrant(membership, null),
                    60000);
            final var scheduleResolutions = new java.util.concurrent.atomic.AtomicInteger();
            final TargetCommandStore.Schedules scheduleProvider = (incoming, position) -> {
                scheduleResolutions.incrementAndGet();
                final var acceptedBinding = new TargetScheduleBinding(
                        incoming.delayMessageId(),
                        incoming.type(),
                        incoming.canonicalBody(),
                        position,
                        physical.id(),
                        initial.domain(),
                        binding.accountingIncarnation(),
                        dispatch.digest(),
                        dispatch.digest(),
                        controls.digest(),
                        membership.digest(),
                        nativeScope.digest(),
                        null);
                return new TargetCommandStore.ScheduleAdmission(
                        StableCode.OK,
                        new TargetScheduleRegistration.Authority(
                                acceptedBinding,
                                physical,
                                destination,
                                capability,
                                profiles,
                                ref -> new TargetMembershipAuthority.AppliedGrant(membership, null),
                                60000),
                        TargetOrderState.OrderingContract.ADMISSION_WATERMARK);
            };
            final var initialCommands = new TargetCommandStore(backend, scope, lineage, 16, 1);
            final var initialPolicy = new TargetCommandStore.Policy(
                    scope,
                    1000,
                    1000,
                    10,
                    java.util.Set.of(schedule.protocolTuple()),
                    new TargetCommandStore.DeliveryWindow(10_000, 1, 100_000));
            final var initialPlan = initialCommands.prepareFirst(
                    budget(),
                    schedule,
                    scheduleAt,
                    initialPolicy,
                    (reader, bound, source) -> false,
                    (reader, bound) -> java.util.Optional.empty(),
                    (incoming, source) -> new TargetCommandStore.ScheduleAdmission(
                            StableCode.OK, scheduleAuthority, TargetOrderState.OrderingContract.ADMISSION_WATERMARK),
                    noProofs());
            final long beforeScheduleFailure = store.latestSequenceNumber();
            assertThrows(
                    IllegalStateException.class,
                    () -> initialCommands.commit(initialPlan, (a, b, c) -> {
                        throw new IllegalStateException("capacity unavailable");
                    }));
            assertEquals(beforeScheduleFailure, store.latestSequenceNumber());
            assertNull(store.get(ColumnFamily.ID, TargetKeyCodec.message(messageId)));
            assertEquals(
                    StableCode.SCHEDULED,
                    initialCommands
                            .commit(
                                    initialCommands.prepareFirst(
                                            budget(),
                                            schedule,
                                            scheduleAt,
                                            initialPolicy,
                                            (reader, bound, source) -> false,
                                            (reader, bound) -> java.util.Optional.empty(),
                                            (incoming, source) -> new TargetCommandStore.ScheduleAdmission(
                                                    StableCode.OK,
                                                    scheduleAuthority,
                                                    TargetOrderState.OrderingContract.ADMISSION_WATERMARK),
                                            noProofs()),
                                    (a, b, c) -> guard())
                            .stableCode());
            final var message = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.ID, TargetKeyCodec.message(messageId)),
                            TargetMessageRecord.VALUE_TYPE)
                    .payload());
            final var locator = message.locator();
            final var work = message.runtime().timeline();
            final var payload = TargetQuotaPayloadOwner.decode(TargetValueEnvelope.decode(
                            store.get(
                                    ColumnFamily.META,
                                    Bytes.concat(
                                            new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, 1}, messageId.bytes())),
                            TargetQuotaPayloadOwner.VALUE_TYPE)
                    .payload());

            final var emptyPhysical =
                    new CanonicalTargetPartition(physical.resource(), physical.physicalPartition() + 1);
            final var emptyTarget = emptyPhysical.id();
            reopenedCloseTargets[1] = emptyTarget;
            final var emptyGrantRequest = new TargetQuotaGrantControlRequest(
                    new TargetQuotaGrant(
                            scope.forTarget(emptyTarget),
                            bytes(32, 0x61),
                            1,
                            originalGrant.accounting(),
                            new TargetQuotaUsage(new CapacityVector(targetAmounts), 1, 64, 64, 64),
                            originalGrant.tenantPolicyVersion(),
                            originalGrant.tenantPolicyHash()),
                    null,
                    null);
            final var emptyGrantAt =
                    source(scheduleAt, scheduleAt.offset() + 1, scheduleAt.brokerLogAppendTimeEpochMs() + 1);
            final var emptyGrant = signed(emptyGrantRequest, bytes(32, 0x62), actor, keys);
            registrations.register(emptyGrant.control());
            assertEquals(
                    StableCode.OK,
                    grantStore
                            .commit(
                                    grantStore.prepareFirst(
                                            budget(),
                                            emptyGrant.control(),
                                            emptyGrant.mutation(),
                                            emptyGrantAt,
                                            authority(
                                                    registrations,
                                                    keys,
                                                    actor,
                                                    emptyGrantAt,
                                                    emptyGrantRequest,
                                                    (a, b, c, d) -> {})),
                                    (a, b, c) -> guard())
                            .stableCode());
            final var emptyActivation = TargetQuotaGrantActivation.decode(TargetValueEnvelope.decode(
                            store.get(
                                    ColumnFamily.META,
                                    Bytes.concat(
                                            new byte[] {TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG, 1},
                                            emptyGrantRequest.next().scope().keySuffix())),
                            TargetQuotaGrantActivation.VALUE_TYPE)
                    .payload());
            final var emptyQueue = new TargetQueueState(
                    emptyTarget,
                    1,
                    1,
                    TargetQueueState.AdmissionState.OPEN,
                    emptyActivation.allocation().identity().accountingIncarnation(),
                    0,
                    List.of());
            final var emptyQueueAt =
                    source(emptyGrantAt, emptyGrantAt.offset() + 1, emptyGrantAt.brokerLogAppendTimeEpochMs() + 1);
            new TargetMessageStore(backend, 1, 1, 1)
                    .applyAccounted(
                            budget(),
                            reader -> new TargetMessageStore.Input(
                                    List.of(),
                                    List.of(),
                                    List.of(
                                            reader.replace(
                                                    ColumnFamily.META,
                                                    TargetKeyCodec.identity(emptyTarget),
                                                    CanonicalTargetPartition.VALUE_TYPE,
                                                    emptyPhysical.canonicalBytes()),
                                            reader.replace(
                                                    ColumnFamily.META,
                                                    TargetKeyCodec.state(emptyTarget),
                                                    TargetQueueState.VALUE_TYPE,
                                                    emptyQueue.canonicalBytes()))),
                            new TargetSourceAccounting(
                                    scope,
                                    lineage,
                                    emptyQueueAt,
                                    Bytes.sha256(Bytes.utf8("empty-close-target-queue-fixture")),
                                    16,
                                    1,
                                    1),
                            (a, b, c) -> guard());
            final var emptyCloseAt =
                    source(emptyQueueAt, emptyQueueAt.offset() + 1, emptyQueueAt.brokerLogAppendTimeEpochMs() + 1);
            final var emptyCloseRequest = new TargetCloseRequest(
                    emptyTarget,
                    List.of(new TargetCloseRequest.ShardTarget(
                            scope.shard(), emptyQueue.accountingIncarnation(), emptyQueue.controlVersion())),
                    new CloseLaneRequest(
                            new ControlReason(ControlReasonKind.OPERATOR_REQUEST, null, null),
                            ClosePolicy._FREEZE_UNADMITTED_AND_PRESERVE_ADMITTED,
                            false,
                            AcknowledgementSet.empty()));
            final var emptySignedClose = signedClose(
                    emptyCloseRequest, bytes(32, 0x63), actor, keys, emptyCloseAt.brokerLogAppendTimeEpochMs() + 2000);
            registrations.register(emptySignedClose.control());
            final var emptyCloseStore = new TargetCloseStore(backend, scope, lineage, 16, 1);
            assertEquals(
                    StableCode.OK,
                    emptyCloseStore
                            .commit(
                                    emptyCloseStore.prepareFirst(
                                            budget(),
                                            emptySignedClose.control(),
                                            emptySignedClose.mutation(),
                                            emptyCloseAt,
                                            new TargetCloseVerifier.Authority(
                                                    registrations,
                                                    (version, position) -> keys.getPublic(),
                                                    (actualScope, requestToClose, position, queueToClose) -> {
                                                        assertEquals(scope, actualScope);
                                                        assertEquals(emptyTarget, requestToClose.target());
                                                        assertEquals(
                                                                emptyQueue.controlVersion(),
                                                                queueToClose.controlVersion());
                                                    },
                                                    actor,
                                                    prepared -> true)),
                                    (a, b, c) -> guard())
                            .stableCode());
            final var emptyWorkClasses = workClasses();
            final var emptyCursorDelta = new java.util.concurrent.atomic.AtomicReference<TargetQuotaDelta>();
            final var emptyWriteFails = new java.util.concurrent.atomic.AtomicBoolean(true);
            final var emptyCloseGc = new TargetReservationClosureWorkClassExecutor(
                    emptyWorkClasses,
                    backend,
                    scope,
                    lineage,
                    1,
                    emptyCloseStore.reservationControls((reader, bound) -> java.util.Optional.empty()),
                    new TargetReservationClosureWorkClassExecutor.Limits(4096, 250_000, 60_000_000_000L),
                    () -> {},
                    (a, b) -> guard(),
                    (a, b, c) -> {
                        if (emptyWriteFails.get()) {
                            throw new IllegalStateException("empty Close capacity unavailable");
                        }
                        return guard();
                    },
                    ignored -> {
                        throw new AssertionError("empty Target has no closure candidate");
                    },
                    ignored -> {
                        throw new AssertionError("empty Target has no expiry candidate");
                    },
                    emptyCursorDelta::set,
                    System::nanoTime);
            final long beforeEmptyCompletion = store.latestSequenceNumber();
            final long sourceSequenceBeforeEmptyCompletion = store.shardMutationSequence();
            final var rejectedEmptyGc = emptyCloseGc.submit(
                    new TargetReservationClosureWorkClassExecutor.Request(scope.shard(), emptyTarget, bytes(16, 0xd3)));
            emptyWorkClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            assertEquals(
                    TargetReservationClosureWorkClassExecutor.Kind.FAILED,
                    rejectedEmptyGc.result().orElseThrow().kind());
            assertEquals(beforeEmptyCompletion, store.latestSequenceNumber());
            emptyWriteFails.set(false);
            final var emptyGcStep = emptyCloseGc.submit(
                    new TargetReservationClosureWorkClassExecutor.Request(scope.shard(), emptyTarget, bytes(16, 0xd4)));
            emptyWorkClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            assertEquals(
                    TargetReservationClosureWorkClassExecutor.Kind.RESERVATIONS_COMPLETE,
                    emptyGcStep.result().orElseThrow().kind());
            assertEquals(5, store.latestSequenceNumber() - beforeEmptyCompletion);
            assertEquals(sourceSequenceBeforeEmptyCompletion, store.shardMutationSequence());
            assertEquals(emptyCloseAt, store.appliedShardLogPosition());
            assertTrue(emptyCursorDelta.get().mutation().reservationCloseCursor());
            final var emptyCursor = com.nereusstream.delay.protocol.TargetCloseCursorRecord.decodeForStore(
                    TargetKeyCodec.closeCursor(emptyTarget),
                    TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.META, TargetKeyCodec.closeCursor(emptyTarget)),
                                    com.nereusstream.delay.protocol.TargetCloseCursorRecord.VALUE_TYPE)
                            .payload(),
                    TargetCloseRecord.decodeForStore(
                            TargetKeyCodec.close(emptyTarget),
                            TargetValueEnvelope.decode(
                                            store.get(ColumnFamily.META, TargetKeyCodec.close(emptyTarget)),
                                            TargetCloseRecord.VALUE_TYPE)
                                    .payload(),
                            scope.shard(),
                            lineage),
                    lineage);
            assertTrue(emptyCursor.complete());
            assertEquals(2, emptyCursor.revision());
            assertNull(emptyCursor.afterMessageId());
            final var repeatedEmptyGc = emptyCloseGc.submit(
                    new TargetReservationClosureWorkClassExecutor.Request(scope.shard(), emptyTarget, bytes(16, 0xd5)));
            emptyWorkClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            assertEquals(
                    TargetReservationClosureWorkClassExecutor.Kind.ALREADY_COMPLETE,
                    repeatedEmptyGc.result().orElseThrow().kind());
            assertEquals(beforeEmptyCompletion + 5, store.latestSequenceNumber());
            final var completedSweep = emptyCloseGc.submit(
                    new TargetReservationClosureWorkClassExecutor.SweepRequest(scope.shard(), bytes(16, 0xd6)));
            assertThrows(
                    IllegalStateException.class,
                    () -> emptyCloseGc.submit(new TargetReservationClosureWorkClassExecutor.SweepRequest(
                            scope.shard(), bytes(16, 0xd7))));
            emptyWorkClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            assertEquals(
                    TargetReservationClosureWorkClassExecutor.Kind.SKIPPED_COMPLETE,
                    completedSweep.result().orElseThrow().kind());
            final var wrappedSweep = emptyCloseGc.submit(
                    new TargetReservationClosureWorkClassExecutor.SweepRequest(scope.shard(), bytes(16, 0xd7)));
            emptyWorkClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            assertEquals(
                    TargetReservationClosureWorkClassExecutor.Kind.SWEEP_COMPLETE,
                    wrappedSweep.result().orElseThrow().kind());
            assertEquals(beforeEmptyCompletion + 5, store.latestSequenceNumber());
            final var sweepStore = new TargetReservationClosureStore(
                    backend,
                    scope,
                    lineage,
                    1,
                    emptyCloseStore.reservationControls((reader, bound) -> java.util.Optional.empty()));
            final var inspectedSweep = sweepStore.discoverNextTarget(budget(), null, (a, b) -> guard());
            assertEquals(emptyTarget, inspectedSweep.target().orElseThrow());
            assertEquals(TargetReservationClosureStore.Progress.COMPLETE, inspectedSweep.progress());
            assertThrows(IllegalArgumentException.class, () -> new TargetReservationClosureStore(
                            backend,
                            scope,
                            lineage,
                            1,
                            emptyCloseStore.reservationControls((reader, bound) -> java.util.Optional.empty()))
                    .discoverNextTarget(budget(), inspectedSweep.nextCursor(), (a, b) -> guard()));
            assertThrows(
                    com.nereusstream.delay.store.ReadIncompleteException.class,
                    () -> sweepStore.discoverNextTarget(
                            new BoundedReadBudget(2, 100_000, 60_000_000_000L, System::nanoTime),
                            null,
                            (a, b) -> guard()));

            final var assignment = new SourceAssignment(
                    scope.shard(),
                    bytes(32, 0x43),
                    1,
                    new KafkaActivationBarrier(
                            scope.shard(),
                            scheduleAt.authenticatedClusterId(),
                            scheduleAt.nativeTopicUuid(),
                            scheduleAt.offset()));
            final var delegateLeases = new InMemoryOwnerLeaseStore();
            final var loseTransitionResponse = new java.util.concurrent.atomic.AtomicBoolean();
            final var loseReleaseResponse = new java.util.concurrent.atomic.AtomicBoolean();
            final var throwTransitionAfterCommit = new java.util.concurrent.atomic.AtomicBoolean();
            final var throwReleaseAfterCommit = new java.util.concurrent.atomic.AtomicBoolean();
            final var leases = new OxiaOwnerLeaseStore(new OwnerLeaseStore() {
                @Override
                public java.util.Optional<OwnerLease> acquire(
                        com.nereusstream.delay.protocol.ShardId shard,
                        String ownerId,
                        long nowEpochMs,
                        long leaseDurationMs) {
                    return delegateLeases.acquire(shard, ownerId, nowEpochMs, leaseDurationMs);
                }

                @Override
                public java.util.Optional<OwnerLease> acquire(
                        SourceAssignment assigned,
                        String ownerId,
                        byte[] sessionIdentity,
                        long nowEpochMs,
                        long leaseDurationMs) {
                    return delegateLeases.acquire(assigned, ownerId, sessionIdentity, nowEpochMs, leaseDurationMs);
                }

                @Override
                public java.util.Optional<OwnerLease> renew(
                        OwnerLease expected, long nowEpochMs, long leaseDurationMs) {
                    return delegateLeases.renew(expected, nowEpochMs, leaseDurationMs);
                }

                @Override
                public boolean release(OwnerLease expected) {
                    final boolean released = delegateLeases.release(expected);
                    if (released && throwReleaseAfterCommit.compareAndSet(true, false)) {
                        throw new IllegalStateException("simulated release response loss after CAS");
                    }
                    return released && !loseReleaseResponse.compareAndSet(true, false);
                }

                @Override
                public java.util.Optional<OwnerLease> transition(OwnerLease expected, ShardLifecycleState nextState) {
                    final var transitioned = delegateLeases.transition(expected, nextState);
                    if (transitioned.isPresent() && throwTransitionAfterCommit.compareAndSet(true, false)) {
                        throw new IllegalStateException("simulated transition response loss after CAS");
                    }
                    return transitioned.isPresent() && loseTransitionResponse.compareAndSet(true, false)
                            ? java.util.Optional.empty()
                            : transitioned;
                }

                @Override
                public java.util.Optional<OwnerLease> current(com.nereusstream.delay.protocol.ShardId shard) {
                    return delegateLeases.current(shard);
                }
            });
            final var active = leases.transition(
                            leases.acquire(assignment, "cancel-worker", bytes(32, 0x44), 1, 10000)
                                    .orElseThrow(),
                            ShardLifecycleState.ACTIVE_FOR_COMMANDS)
                    .orElseThrow();
            reopenOwner[0] = new ReopenOwner(
                    assignment,
                    leases,
                    active.ownerEpoch(),
                    loseTransitionResponse,
                    loseReleaseResponse,
                    throwTransitionAfterCommit,
                    throwReleaseAfterCommit);
            store.recordOpenedOwnerEpoch(active.ownerEpoch());
            TargetClaimRecord claim = null;
            if (claimed) {
                final var actualQueue = TargetQueueState.decode(TargetValueEnvelope.decode(
                                store.get(ColumnFamily.META, TargetKeyCodec.state(binding.target())),
                                TargetQueueState.VALUE_TYPE)
                        .payload());
                final var claims = new TargetClaimStore(backend, scope, lineage, 1, (kind, delta) -> {});
                final var plan = claims.prepareClaim(
                        budget(),
                        actualQueue.domains().getFirst().ordinaryHead(),
                        new OwnerIdentity(bytes(16, 0x72), bytes(16, 0x73), active.ownerEpoch(), bytes(32, 0x74)),
                        message.deliverAtEpochMs(),
                        message.deliverAtEpochMs() + 1000,
                        100,
                        bytes(32, 0x71));
                claim = plan.claim();
                claims.commit(plan, (a, b, c) -> guard());
            }
            final var before = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.ID, message.encodedKey()), TargetMessageRecord.VALUE_TYPE)
                    .payload());
            final var at =
                    source(emptyCloseAt, emptyCloseAt.offset() + 1, emptyCloseAt.brokerLogAppendTimeEpochMs() + 1);
            final var command = rescheduled
                    ? reschedule(message.locator().messageId(), at, 1)
                    : cancel(message.locator().messageId(), at, 1);
            final var expectedCode = rescheduled ? StableCode.SUPERSEDED : StableCode.CANCELED;
            final var policy = new TargetCommandStore.Policy(
                    scope,
                    1000,
                    1000,
                    10,
                    java.util.Set.of(command.protocolTuple()),
                    new TargetCommandStore.DeliveryWindow(10_000, 1, 100_000));
            final var firstStore = new TargetCommandStore(backend, scope, lineage, 16, 1);
            final var failed = firstStore.prepareFirst(
                    budget(),
                    command,
                    at,
                    policy,
                    (reader, bound, source) -> false,
                    (reader, bound) -> java.util.Optional.empty(),
                    (incoming, source) -> {
                        throw new AssertionError("unexpected first Schedule");
                    },
                    noProofs());
            final long nativeBeforeFailure = store.latestSequenceNumber();
            assertThrows(
                    IllegalStateException.class,
                    () -> firstStore.commit(failed, (a, b, c) -> {
                        throw new IllegalStateException("capacity unavailable");
                    }));
            assertEquals(nativeBeforeFailure, store.latestSequenceNumber());
            assertArrayEquals(
                    before.canonicalBytes(),
                    TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.ID, message.encodedKey()), TargetMessageRecord.VALUE_TYPE)
                            .payload());
            final var reservationClosures = new java.util.HashMap<String, TargetReservationControls.Closure>();
            final var closeStore = new TargetCloseStore(backend, scope, lineage, 16, 1);
            final var closeResolutions = new java.util.concurrent.atomic.AtomicInteger();
            final var closeAuthority = new TargetCloseVerifier.Authority(
                    registrations,
                    (version, source) -> keys.getPublic(),
                    (actualScope, closeRequest, source, queue) -> {
                        assertEquals(scope, actualScope);
                        assertEquals(physical.id(), closeRequest.target());
                        assertEquals(1, closeRequest.shards().size());
                        assertEquals(
                                scope.shard(), closeRequest.shards().getFirst().shard());
                    },
                    actor,
                    prepared -> true);
            final TargetReservationControls.Authority reservationControls =
                    closeStore.reservationControls((reader, bound) ->
                            java.util.Optional.ofNullable(reservationClosures.get(Bytes.hex(bound.digest()))));
            final var resolutions = new java.util.concurrent.atomic.AtomicInteger();
            final var proofKeys =
                    java.security.KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final var wrongProofKeys =
                    java.security.KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final var trustSet = new PayloadProofTrustSetSemantic(
                    1, List.of(PayloadProofVerifierKey.fromPublicKey(1, proofKeys.getPublic(), 0, Long.MAX_VALUE)));
            final var proofControls = PayloadProofTrustSetControlState.empty().activate(trustSet.ref(), grantAt);
            final var proofResolutions = new java.util.concurrent.atomic.AtomicInteger();
            final var runtime = new TargetSourceApplyRuntime(
                    initialized,
                    store,
                    assignment,
                    active,
                    new TargetSourceApplyRuntime.Authorities(
                            leases,
                            SourceReplaySuccessor.strictKafka(),
                            entry -> {
                                throw new AssertionError("Command resolved grant");
                            },
                            entry -> {
                                return new TargetSourceApplyRuntime.FenceControl(
                                        (actualScope, author, mutation, position) ->
                                                new TargetTimeFenceVerifier.Authorization(
                                                        keys.getPublic(), 10, 5, (a, b, c, proof) -> true),
                                        (a, b, c) -> guard());
                            },
                            entry -> {
                                closeResolutions.incrementAndGet();
                                final var closeBody =
                                        TargetCloseBody.decode(entry.mutation().canonicalBody());
                                return new TargetSourceApplyRuntime.CloseControl(
                                        registrations
                                                .find(closeBody.controlRef().operationId())
                                                .orElseThrow(),
                                        closeAuthority,
                                        (a, b, c) -> guard());
                            },
                            entry -> {
                                throw new AssertionError("unexpected membership issue authority");
                            },
                            (a, b, c) -> guard(),
                            (a, b) -> guard(),
                            entry -> {
                                resolutions.incrementAndGet();
                                return new TargetSourceApplyRuntime.CommandControl(
                                        policy,
                                        (reader, bound, source) -> {
                                            if (bound.commandType() == CommandType.SCHEDULE) {
                                                assertArrayEquals(binding.canonicalBytes(), bound.canonicalBytes());
                                            } else {
                                                assertEquals(CommandType.PREPARE_LARGE_SCHEDULE, bound.commandType());
                                                assertArrayEquals(membership.digest(), bound.membershipGrantRef());
                                            }
                                            return false;
                                        },
                                        reservationControls,
                                        scheduleProvider,
                                        (bound, source) -> {
                                            proofResolutions.incrementAndGet();
                                            return new TargetCommandStore.PayloadProofAuthority(
                                                    trustSet, proofControls);
                                        },
                                        (a, b, c) -> guard());
                            }),
                    new TargetSourceApplyRuntime.Limits(4096, 32L << 20, 60_000_000_000L, 16, 1),
                    System::nanoTime);
            final var entries = new java.util.ArrayDeque<SourceRecordConsumer.PolledSourceRecord>();
            final SourceRecordConsumer consumer = () -> java.util.Optional.ofNullable(entries.poll());
            final var workerClasses = workClasses();
            final var loop = new WorkerSourceApplyLoop(consumer, workerClasses, runtime);
            final var completed = apply(loop, entries, command, at);
            assertEquals(SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED, completed.status());
            assertEquals(
                    expectedCode, completed.appliedOutcome().commandResult().stableCode());
            assertEquals(1, resolutions.get());
            final var after = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.ID, message.encodedKey()), TargetMessageRecord.VALUE_TYPE)
                    .payload());
            assertEquals(
                    rescheduled ? GenerationAggregateState.SCHEDULED : GenerationAggregateState.CANCELED,
                    after.aggregateState());
            assertEquals(before.stateVersion() + 1, after.stateVersion());
            assertEquals(
                    rescheduled ? 1 : before.runtime().runtimeRevision() + 1,
                    after.runtime().runtimeRevision());
            assertEquals(
                    before.locator().generation() + (rescheduled ? 1 : 0),
                    after.locator().generation());
            if (rescheduled) {
                assertEquals(at.brokerLogAppendTimeEpochMs() + 100, after.deliverAtEpochMs());
                assertEquals(at.brokerLogAppendTimeEpochMs() + 2000, after.expireAtEpochMs());
                assertArrayEquals(at.canonicalBytes(), after.scheduleSource().canonicalBytes());
                assertArrayEquals(
                        before.locator().scheduleBindingDigest(),
                        after.locator().scheduleBindingDigest());
                assertArrayEquals(
                        payload.canonicalBytes(),
                        TargetValueEnvelope.decode(
                                        store.get(ColumnFamily.META, payload.key()), TargetQuotaPayloadOwner.VALUE_TYPE)
                                .payload());
                org.junit.jupiter.api.Assertions.assertNotNull(store.get(
                        ColumnFamily.TIMELINE, after.runtime().timeline().ordinaryKey()));
                org.junit.jupiter.api.Assertions.assertNotNull(store.get(
                        ColumnFamily.TIMELINE, after.runtime().timeline().nativeKey()));
                org.junit.jupiter.api.Assertions.assertNotNull(store.get(
                        ColumnFamily.TIMELINE,
                        new TargetExpiryRef(after.locator(), after.expireAtEpochMs()).encodedKey()));
            }
            assertNull(store.get(ColumnFamily.TIMELINE, work.ordinaryKey()));
            assertNull(store.get(ColumnFamily.TIMELINE, work.nativeKey()));
            assertNull(store.get(
                    ColumnFamily.TIMELINE, new TargetExpiryRef(locator, message.expireAtEpochMs()).encodedKey()));
            if (claim != null) {
                assertNull(store.get(ColumnFamily.INFLIGHT, claim.key()));
                assertNull(store.get(ColumnFamily.META, claim.chargeKey()));
            }
            final var retained = TargetQuotaPayloadOwner.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.META, payload.key()), TargetQuotaPayloadOwner.VALUE_TYPE)
                    .payload());
            assertEquals(
                    rescheduled ? TargetQuotaPayloadOwner.Phase.ACTIVE : TargetQuotaPayloadOwner.Phase.RETAINED,
                    retained.phase());
            assertEquals(payload.primaryIdentity(), retained.primaryIdentity());
            final var terminal = TargetTerminalGenerationRecord.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.TERMINAL, TargetTerminalGenerationRecord.key(locator)),
                            TargetTerminalGenerationRecord.VALUE_TYPE)
                    .payload());
            terminal.requireOwner(retained);
            assertEquals(expectedCode, terminal.terminalCode());
            final var aggregate = TargetQuotaAggregate.decode(TargetValueEnvelope.decode(
                            store.get(
                                    ColumnFamily.META,
                                    backend.prepareRead(budget(), reader -> reader.aggregate()
                                                    .key())
                                            .value()),
                            TargetQuotaAggregate.VALUE_TYPE)
                    .payload());
            assertEquals(
                    rescheduled ? message.payloadLength() : 0,
                    aggregate.usage().resources().amount(CapacityDimension.PENDING_PAYLOAD_BYTES));
            assertEquals(
                    rescheduled ? 0 : message.payloadLength(),
                    aggregate.usage().resources().amount(CapacityDimension.RETAINED_BYTES));
            final long nativeBeforeReplay = store.latestSequenceNumber();
            assertEquals(
                    expectedCode,
                    apply(loop, entries, command, at)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertEquals(nativeBeforeReplay, store.latestSequenceNumber());
            assertEquals(1, resolutions.get());
            final var later = source(at, at.offset() + 1, at.brokerLogAppendTimeEpochMs() + 1);
            assertEquals(
                    rescheduled ? StableCode.CANCELED : StableCode.ALREADY_CANCELED,
                    apply(loop, entries, cancel(locator.messageId(), later, 2), later)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertEquals(2, resolutions.get());
            final var missingAt = source(later, later.offset() + 1, later.brokerLogAppendTimeEpochMs() + 1);
            final var missingId = new DelayMessageId(
                    cancel(locator.messageId(), missingAt, 99).commandId().bytes());
            assertEquals(
                    StableCode.NOT_FOUND,
                    apply(loop, entries, cancel(missingId, missingAt, 100), missingAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertEquals(3, resolutions.get());
            final var freshAt = source(missingAt, missingAt.offset() + 1, missingAt.brokerLogAppendTimeEpochMs() + 1);
            final var fresh = schedule(intent, locator.messageId(), freshAt, 200);
            assertEquals(
                    StableCode.SCHEDULED,
                    apply(loop, entries, fresh, freshAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            final long afterFresh = store.latestSequenceNumber();
            assertEquals(
                    StableCode.SCHEDULED,
                    apply(loop, entries, fresh, freshAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertEquals(afterFresh, store.latestSequenceNumber());
            final var rejectedAt = source(freshAt, freshAt.offset() + 1, freshAt.brokerLogAppendTimeEpochMs() + 1);
            final var overQuota = schedule(intent, locator.messageId(), rejectedAt, 300);
            assertEquals(
                    StableCode.HARD_QUOTA_EXCEEDED,
                    apply(loop, entries, overQuota, rejectedAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertNull(store.get(ColumnFamily.ID, TargetKeyCodec.message(overQuota.delayMessageId())));
            assertNull(store.get(
                    ColumnFamily.META,
                    Bytes.concat(
                            new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, 1},
                            overQuota.delayMessageId().bytes())));
            final long afterQuota = store.latestSequenceNumber();
            assertEquals(
                    StableCode.HARD_QUOTA_EXCEEDED,
                    apply(loop, entries, overQuota, rejectedAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertEquals(afterQuota, store.latestSequenceNumber());
            assertEquals(5, resolutions.get());
            assertEquals(2, scheduleResolutions.get());
            final var rejectedBinding = new TargetScheduleBinding(
                    overQuota.delayMessageId(),
                    CommandType.SCHEDULE,
                    overQuota.canonicalBody(),
                    rejectedAt,
                    physical.id(),
                    initial.domain(),
                    binding.accountingIncarnation(),
                    dispatch.digest(),
                    dispatch.digest(),
                    controls.digest(),
                    membership.digest(),
                    nativeScope.digest(),
                    null);
            assertNull(store.get(ColumnFamily.ID, rejectedBinding.encodedKey()));
            final var conflictAt =
                    source(rejectedAt, rejectedAt.offset() + 1, rejectedAt.brokerLogAppendTimeEpochMs() + 1);
            final var conflictBody = new ScheduleCommandBody(
                    fresh.delayMessageId(), conflictAt.brokerLogAppendTimeEpochMs() + 1000, intent);
            final var conflictId =
                    cancel(fresh.delayMessageId(), conflictAt, 400).commandId();
            final var conflict = new PreparedCommand(
                    scope.shard(),
                    conflictId,
                    fresh.delayMessageId(),
                    CommandType.SCHEDULE,
                    fresh.protocolTuple(),
                    conflictBody.retryUntilEpochMs(),
                    conflictBody.canonicalBytes(),
                    CommandHash.compute(
                            fresh.protocolTuple(),
                            CommandType.SCHEDULE,
                            conflictId,
                            fresh.delayMessageId(),
                            conflictBody.retryUntilEpochMs(),
                            conflictBody.canonicalBytes()));
            final var conflicted =
                    apply(loop, entries, conflict, conflictAt).appliedOutcome().commandResult();
            assertEquals(StableCode.DELAY_MESSAGE_ID_CONFLICT, conflicted.stableCode());
            assertEquals(ApplyStatus.REJECTED, conflicted.applyStatus());
            assertEquals(0, conflicted.generation());
            assertEquals(1, conflicted.stateVersion());
            assertEquals(MessageStatus.SCHEDULED, conflicted.messageStatus());
            assertEquals(2, scheduleResolutions.get());
            final var prepareAt =
                    source(conflictAt, conflictAt.offset() + 1, conflictAt.brokerLogAppendTimeEpochMs() + 1);
            final var modelPrepare = PrepareLargeScheduleBody.decode(
                    vector("target-binding-channel-vectors.properties", "body.prepare"));
            final var prepareIntent = CanonicalScheduleIntent.forPrepare(
                    intent.profile(),
                    intent.retryPolicy(),
                    intent.deliverAtEpochMs(),
                    intent.expireAtEpochMs(),
                    intent.deliveryMode(),
                    intent.orderingMode(),
                    intent.orderingKey(),
                    intent.adapterMetadata(),
                    intent.businessKey(),
                    intent.eventTimeEpochMs(),
                    intent.nativeDeliveryPolicy());
            final var prepareId = cancel(locator.messageId(), prepareAt, 500).commandId();
            final var reservedMessage = new DelayMessageId(
                    cancel(locator.messageId(), prepareAt, 1500).commandId().bytes());
            final var prepareBody = new PrepareLargeScheduleBody(
                    reservedMessage,
                    prepareAt.brokerLogAppendTimeEpochMs() + 1000,
                    prepareIntent,
                    100,
                    bytes(32, 0xd1),
                    500,
                    trustSet.ref(),
                    modelPrepare.objectStoreProfile());
            final var prepare = new PreparedCommand(
                    scope.shard(),
                    prepareId,
                    reservedMessage,
                    CommandType.PREPARE_LARGE_SCHEDULE,
                    fresh.protocolTuple(),
                    prepareBody.retryUntilEpochMs(),
                    prepareBody.canonicalBytes(),
                    CommandHash.compute(
                            fresh.protocolTuple(),
                            CommandType.PREPARE_LARGE_SCHEDULE,
                            prepareId,
                            reservedMessage,
                            prepareBody.retryUntilEpochMs(),
                            prepareBody.canonicalBytes()));
            assertEquals(
                    StableCode.OK,
                    apply(loop, entries, prepare, prepareAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertNull(store.get(ColumnFamily.ID, TargetKeyCodec.message(reservedMessage)));
            final byte[] reservationKey =
                    Bytes.concat(new byte[] {TargetKeyCodec.RESERVATION_TAG, 1}, reservedMessage.bytes());
            final var reserved = TargetReservationRecord.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.ID, reservationKey), TargetReservationRecord.VALUE_TYPE)
                    .payload());
            assertEquals(PayloadReservationStatus.RESERVED, reserved.status());
            assertArrayEquals(
                    reserved.canonicalBytes(),
                    TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.ID, reserved.lookupKey()),
                                    TargetReservationRecord.VALUE_TYPE)
                            .payload());
            assertArrayEquals(
                    reserved.canonicalBytes(),
                    TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.TIMELINE, reserved.expiryKey()),
                                    TargetReservationRecord.VALUE_TYPE)
                            .payload());
            final byte[] reservedOwnerKey =
                    Bytes.concat(new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, 1}, reservedMessage.bytes());
            final var reservedOwner = TargetQuotaPayloadOwner.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.META, reservedOwnerKey), TargetQuotaPayloadOwner.VALUE_TYPE)
                    .payload());
            reserved.requireOwner(reservedOwner);
            assertEquals(TargetQuotaPayloadOwner.Phase.RESERVED, reservedOwner.phase());
            final var queries = new TargetReservationQueryStore(backend, scope, lineage, reservationControls);
            final var location = new TargetReservationQueryStore.ReceiptLocation(
                    prepareBody.objectStoreProfile(), Bytes.utf8("bucket"), Bytes.utf8("object"));
            final long beforeQueries = store.latestSequenceNumber();
            final var reservedSnapshot = queries.complete(
                            queries.prepare(budget(), reserved.reservationId(), (a, b) -> guard()), (a, b) -> guard())
                    .orElseThrow();
            assertEquals(
                    PayloadReservationStatus.RESERVED,
                    reservedSnapshot.reservation().status());
            assertEquals(TargetQuotaPayloadOwner.Phase.RESERVED, reservedSnapshot.payloadPhase());
            assertEquals(prepareAt, reservedSnapshot.readSource());
            final var prepareReceipt = reservedSnapshot.receipt(location);
            assertEquals(prepareAt, prepareReceipt.appliedSourcePosition());
            assertEquals(1, prepareReceipt.stateVersion());
            assertTrue(
                    queries.complete(queries.prepare(budget(), bytes(32, 0xf6), (a, b) -> guard()), (a, b) -> guard())
                            .isEmpty());
            assertThrows(
                    IllegalStateException.class,
                    () -> queries.complete(
                            queries.prepare(budget(), reserved.reservationId(), (a, b) -> guard()), (a, b) -> {
                                throw new IllegalStateException("Owner lost");
                            }));
            final var localReadExhaustion = assertThrows(
                    com.nereusstream.delay.store.ReadIncompleteException.class,
                    () -> queries.prepare(
                            new BoundedReadBudget(1, 32L << 20, 60_000_000_000L, System::nanoTime),
                            reserved.reservationId(),
                            (a, b) -> guard()));
            final var queryGuards = new java.util.concurrent.atomic.AtomicInteger();
            final var queryGuardChecks = new java.util.concurrent.atomic.AtomicInteger();
            final var queryGuardCloses = new java.util.concurrent.atomic.AtomicInteger();
            final var queryOwnerCurrent = new java.util.concurrent.atomic.AtomicBoolean(true);
            final TargetStoreBackend.ReadAuthority queryAuthority = (a, b) -> {
                queryGuards.incrementAndGet();
                return new TargetStoreBackend.CommitGuard() {
                    public void requireCurrent() {
                        queryGuardChecks.incrementAndGet();
                        if (!queryOwnerCurrent.get()) {
                            throw new IllegalStateException("query Owner lost");
                        }
                    }

                    public void close() {
                        queryGuardCloses.incrementAndGet();
                    }
                };
            };
            final var prepareGuardHeld = new java.util.concurrent.atomic.AtomicBoolean();
            final var guardedPrepareQueries =
                    new TargetReservationQueryStore(backend, scope, lineage, (reader, bound) -> {
                        assertTrue(prepareGuardHeld.get(), "closure authority ran outside the query prepare guard");
                        return java.util.Optional.empty();
                    });
            final TargetStoreBackend.ReadAuthority prepareReadAuthority = (a, b) -> {
                assertEquals(false, prepareGuardHeld.getAndSet(true));
                return new TargetStoreBackend.CommitGuard() {
                    public void requireCurrent() {
                        assertTrue(prepareGuardHeld.get());
                    }

                    public void close() {
                        assertTrue(prepareGuardHeld.getAndSet(false));
                    }
                };
            };
            final var guardedReadPlan =
                    guardedPrepareQueries.prepare(budget(), reserved.reservationId(), prepareReadAuthority);
            assertEquals(false, prepareGuardHeld.get());
            assertEquals(
                    prepareReceipt,
                    guardedPrepareQueries
                            .complete(guardedReadPlan, prepareReadAuthority)
                            .orElseThrow()
                            .receipt(location));
            assertEquals(false, prepareGuardHeld.get());
            final var queryLimits = new TargetReservationQueryWorkClassExecutor.Limits(2048, 100_000, 60_000_000_000L);
            final var queryExecutor = new TargetReservationQueryWorkClassExecutor(
                    workerClasses, queries, queryLimits, () -> {}, queryAuthority, System::nanoTime);
            final var queuedQuery = queryExecutor.submit(new TargetReservationQueryWorkClassExecutor.Request(
                    scope.shard(), bytes(16, 0xe1), reserved.reservationId()));
            assertTrue(queuedQuery.result().isEmpty());
            assertEquals(WorkClass.QUERY, queuedQuery.task().workClass());
            assertTrue(queuedQuery.task().bytes() > queryLimits.maximumBytes());
            assertEquals(0, queryGuards.get());
            assertThrows(
                    RuntimeException.class,
                    () -> queryExecutor.submit(new TargetReservationQueryWorkClassExecutor.Request(
                            scope.shard(), bytes(16, 0xe2), reserved.reservationId())));
            assertEquals(0, queryGuards.get());
            workerClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            assertEquals(
                    TargetReservationQueryWorkClassExecutor.Kind.COMPLETED,
                    queuedQuery.result().orElseThrow().kind());
            assertEquals(
                    prepareReceipt,
                    queuedQuery.result().orElseThrow().snapshot().orElseThrow().receipt(location));
            assertEquals(1, queryGuards.get());
            assertEquals(2, queryGuardChecks.get());
            assertEquals(1, queryGuardCloses.get());
            final var lostQuery = queryExecutor.submit(new TargetReservationQueryWorkClassExecutor.Request(
                    scope.shard(), bytes(16, 0xe3), reserved.reservationId()));
            queryOwnerCurrent.set(false);
            workerClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            assertEquals(
                    TargetReservationQueryWorkClassExecutor.Kind.FAILED,
                    lostQuery.result().orElseThrow().kind());
            assertTrue(lostQuery.result().orElseThrow().snapshot().isEmpty());
            final var unreadBudget = budget();
            assertThrows(
                    IllegalStateException.class,
                    () -> queries.read(unreadBudget, reserved.reservationId(), queryAuthority));
            assertEquals(0, unreadBudget.actualRecords());
            queryOwnerCurrent.set(true);
            final var limitedExecutor = new TargetReservationQueryWorkClassExecutor(
                    workerClasses,
                    queries,
                    new TargetReservationQueryWorkClassExecutor.Limits(1, 100_000, 60_000_000_000L),
                    () -> {},
                    queryAuthority,
                    System::nanoTime);
            final var incompleteQuery = limitedExecutor.submit(new TargetReservationQueryWorkClassExecutor.Request(
                    scope.shard(), bytes(16, 0xe4), reserved.reservationId()));
            workerClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            assertEquals(
                    TargetReservationQueryWorkClassExecutor.Kind.READ_INCOMPLETE,
                    incompleteQuery.result().orElseThrow().kind());
            final var failedAuthority = new TargetReservationQueryWorkClassExecutor(
                    workerClasses,
                    queries,
                    queryLimits,
                    () -> {},
                    (a, b) -> {
                        throw localReadExhaustion;
                    },
                    System::nanoTime);
            final var authorityQuery = failedAuthority.submit(new TargetReservationQueryWorkClassExecutor.Request(
                    scope.shard(), bytes(16, 0xe5), reserved.reservationId()));
            workerClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            assertEquals(
                    TargetReservationQueryWorkClassExecutor.Kind.FAILED,
                    authorityQuery.result().orElseThrow().kind());
            assertTrue(authorityQuery.result().orElseThrow().failure() instanceof IllegalStateException);
            assertEquals(0, workerClasses.pending(WorkClass.QUERY));
            assertEquals(0, workerClasses.registeredActions());
            final var staleQuery = queries.prepare(budget(), reserved.reservationId(), (a, b) -> guard());
            assertEquals(beforeQueries, store.latestSequenceNumber());

            final long afterPrepare = store.latestSequenceNumber();
            assertEquals(
                    StableCode.OK,
                    apply(loop, entries, prepare, prepareAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertEquals(afterPrepare, store.latestSequenceNumber());
            assertEquals(3, scheduleResolutions.get());
            final var quotaAt = source(prepareAt, prepareAt.offset() + 1, prepareAt.brokerLogAppendTimeEpochMs() + 1);
            final var quotaMessage = new DelayMessageId(
                    cancel(locator.messageId(), quotaAt, 1600).commandId().bytes());
            final var quotaId = cancel(locator.messageId(), quotaAt, 550).commandId();
            final var quotaBody = new PrepareLargeScheduleBody(
                    quotaMessage,
                    quotaAt.brokerLogAppendTimeEpochMs() + 1000,
                    prepareIntent,
                    100,
                    bytes(32, 0xd1),
                    500,
                    trustSet.ref(),
                    modelPrepare.objectStoreProfile());
            final var quotaPrepare = new PreparedCommand(
                    scope.shard(),
                    quotaId,
                    quotaMessage,
                    CommandType.PREPARE_LARGE_SCHEDULE,
                    fresh.protocolTuple(),
                    quotaBody.retryUntilEpochMs(),
                    quotaBody.canonicalBytes(),
                    CommandHash.compute(
                            fresh.protocolTuple(),
                            CommandType.PREPARE_LARGE_SCHEDULE,
                            quotaId,
                            quotaMessage,
                            quotaBody.retryUntilEpochMs(),
                            quotaBody.canonicalBytes()));
            assertEquals(
                    StableCode.HARD_QUOTA_EXCEEDED,
                    apply(loop, entries, quotaPrepare, quotaAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertNull(store.get(
                    ColumnFamily.ID,
                    Bytes.concat(new byte[] {TargetKeyCodec.RESERVATION_TAG, 1}, quotaMessage.bytes())));
            assertNull(store.get(
                    ColumnFamily.META,
                    Bytes.concat(new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, 1}, quotaMessage.bytes())));
            final var reservedUsage = backend.prepareRead(
                            budget(), reader -> reader.aggregate().usage())
                    .value();
            assertEquals(1, reservedUsage.resources().amount(CapacityDimension.RESERVATION_MESSAGES));
            assertEquals(100, reservedUsage.resources().amount(CapacityDimension.RESERVATION_PAYLOAD_BYTES));
            final var versionAt = source(quotaAt, quotaAt.offset() + 1, quotaAt.brokerLogAppendTimeEpochMs() + 1);
            final var wrongVersion = reschedule(reservedMessage, versionAt, 570, new MessagePrecondition(1L, 1L));
            assertEquals(
                    StableCode.VERSION_CONFLICT,
                    apply(loop, entries, wrongVersion, versionAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            final var rescheduleAt =
                    source(versionAt, versionAt.offset() + 1, versionAt.brokerLogAppendTimeEpochMs() + 1);
            final var uncommitted = reschedule(reservedMessage, rescheduleAt, 580, new MessagePrecondition(0L, 1L));
            assertEquals(
                    StableCode.RESERVATION_NOT_COMMITTED,
                    apply(loop, entries, uncommitted, rescheduleAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            final long afterUncommittedWrites = store.latestSequenceNumber();
            assertEquals(
                    StableCode.RESERVATION_NOT_COMMITTED,
                    apply(loop, entries, uncommitted, rescheduleAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertEquals(afterUncommittedWrites, store.latestSequenceNumber());
            assertNull(store.get(ColumnFamily.ID, TargetKeyCodec.message(reservedMessage)));
            assertArrayEquals(
                    reserved.canonicalBytes(),
                    TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.ID, reservationKey), TargetReservationRecord.VALUE_TYPE)
                            .payload());
            assertArrayEquals(
                    reserved.canonicalBytes(),
                    TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.ID, reserved.lookupKey()),
                                    TargetReservationRecord.VALUE_TYPE)
                            .payload());
            assertArrayEquals(
                    reserved.canonicalBytes(),
                    TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.TIMELINE, reserved.expiryKey()),
                                    TargetReservationRecord.VALUE_TYPE)
                            .payload());
            assertArrayEquals(
                    reservedOwner.canonicalBytes(),
                    TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.META, reservedOwnerKey), TargetQuotaPayloadOwner.VALUE_TYPE)
                            .payload());
            final var afterRescheduleUsage = backend.prepareRead(
                            budget(), reader -> reader.aggregate().usage())
                    .value();
            for (var dimension : List.of(
                    CapacityDimension.RESERVATION_MESSAGES,
                    CapacityDimension.RESERVATION_PAYLOAD_BYTES,
                    CapacityDimension.RETAINED_BYTES)) {
                assertEquals(
                        reservedUsage.resources().amount(dimension),
                        afterRescheduleUsage.resources().amount(dimension));
            }
            final var abandonAt =
                    source(rescheduleAt, rescheduleAt.offset() + 1, rescheduleAt.brokerLogAppendTimeEpochMs() + 1);
            final var abandon = cancel(reservedMessage, abandonAt, 600);
            assertEquals(
                    StableCode.PAYLOAD_RESERVATION_ABANDONED,
                    apply(loop, entries, abandon, abandonAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            final var abandoned = TargetReservationRecord.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.ID, reservationKey), TargetReservationRecord.VALUE_TYPE)
                    .payload());
            assertEquals(PayloadReservationStatus.ABANDONED, abandoned.status());
            assertArrayEquals(
                    abandoned.canonicalBytes(),
                    TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.ID, abandoned.lookupKey()),
                                    TargetReservationRecord.VALUE_TYPE)
                            .payload());
            assertEquals(2, abandoned.stateVersion());
            assertEquals(reserved.prepareAnchor(), abandoned.prepareAnchor());
            assertNull(store.get(ColumnFamily.TIMELINE, reserved.expiryKey()));
            final var releasedOwner = TargetQuotaPayloadOwner.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.META, reservedOwnerKey), TargetQuotaPayloadOwner.VALUE_TYPE)
                    .payload());
            abandoned.requireOwner(releasedOwner);
            assertEquals(TargetQuotaPayloadOwner.Phase.RETAINED, releasedOwner.phase());
            assertThrows(IllegalStateException.class, () -> queries.complete(staleQuery, (a, b) -> guard()));
            final var abandonedSnapshot = queries.complete(
                            queries.prepare(budget(), reserved.reservationId(), (a, b) -> guard()), (a, b) -> guard())
                    .orElseThrow();
            assertEquals(
                    PayloadReservationStatus.ABANDONED,
                    abandonedSnapshot.reservation().status());
            assertEquals(abandonAt, abandonedSnapshot.readSource());
            assertEquals(prepareReceipt, abandonedSnapshot.receipt(location));

            final var retainedUsage = backend.prepareRead(
                            budget(), reader -> reader.aggregate().usage())
                    .value();
            assertEquals(0, retainedUsage.resources().amount(CapacityDimension.RESERVATION_MESSAGES));
            assertEquals(0, retainedUsage.resources().amount(CapacityDimension.RESERVATION_PAYLOAD_BYTES));
            assertEquals(
                    reservedUsage.resources().amount(CapacityDimension.RETAINED_BYTES) + 100,
                    retainedUsage.resources().amount(CapacityDimension.RETAINED_BYTES));
            final long afterAbandon = store.latestSequenceNumber();
            assertEquals(
                    StableCode.PAYLOAD_RESERVATION_ABANDONED,
                    apply(loop, entries, abandon, abandonAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertEquals(afterAbandon, store.latestSequenceNumber());
            final var repeatAt = source(abandonAt, abandonAt.offset() + 1, abandonAt.brokerLogAppendTimeEpochMs() + 1);
            assertEquals(
                    StableCode.ALREADY_ABANDONED,
                    apply(loop, entries, cancel(reservedMessage, repeatAt, 601), repeatAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            final var terminalRescheduleAt =
                    source(repeatAt, repeatAt.offset() + 1, repeatAt.brokerLogAppendTimeEpochMs() + 1);
            assertEquals(
                    StableCode.ALREADY_ABANDONED,
                    apply(loop, entries, reschedule(reservedMessage, terminalRescheduleAt, 602), terminalRescheduleAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertArrayEquals(
                    abandoned.canonicalBytes(),
                    TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.ID, reservationKey), TargetReservationRecord.VALUE_TYPE)
                            .payload());
            assertNull(store.get(ColumnFamily.ID, TargetKeyCodec.message(reservedMessage)));
            assertNull(store.get(ColumnFamily.TIMELINE, reserved.expiryKey()));
            final var secondPrepareAt = source(
                    terminalRescheduleAt,
                    terminalRescheduleAt.offset() + 1,
                    terminalRescheduleAt.brokerLogAppendTimeEpochMs() + 1);
            final var secondMessage = new DelayMessageId(
                    cancel(reservedMessage, secondPrepareAt, 1700).commandId().bytes());
            final var secondPrepareId =
                    cancel(reservedMessage, secondPrepareAt, 700).commandId();
            final var secondBody = new PrepareLargeScheduleBody(
                    secondMessage,
                    secondPrepareAt.brokerLogAppendTimeEpochMs() + 1000,
                    prepareIntent,
                    100,
                    bytes(32, 0xd1),
                    500,
                    trustSet.ref(),
                    modelPrepare.objectStoreProfile());
            final var secondPrepare = new PreparedCommand(
                    scope.shard(),
                    secondPrepareId,
                    secondMessage,
                    CommandType.PREPARE_LARGE_SCHEDULE,
                    fresh.protocolTuple(),
                    secondBody.retryUntilEpochMs(),
                    secondBody.canonicalBytes(),
                    CommandHash.compute(
                            fresh.protocolTuple(),
                            CommandType.PREPARE_LARGE_SCHEDULE,
                            secondPrepareId,
                            secondMessage,
                            secondBody.retryUntilEpochMs(),
                            secondBody.canonicalBytes()));
            assertEquals(
                    StableCode.OK,
                    apply(loop, entries, secondPrepare, secondPrepareAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            final var secondKey = Bytes.concat(new byte[] {TargetKeyCodec.RESERVATION_TAG, 1}, secondMessage.bytes());
            final var secondReservation = TargetReservationRecord.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.ID, secondKey), TargetReservationRecord.VALUE_TYPE)
                    .payload());
            final var secondPrepareReceipt = queries.complete(
                            queries.prepare(budget(), secondReservation.reservationId(), (a, b) -> guard()),
                            (a, b) -> guard())
                    .orElseThrow()
                    .receipt(location);
            final var proof = CanonicalPayloadCommitProof.signed(
                    secondReservation.reservationId(),
                    scope.tenantScope(),
                    scope.shard().routeIncarnation().bytes(),
                    scope.shard().partition(),
                    secondMessage,
                    secondBody.objectStoreProfile(),
                    trustSet.version(),
                    1,
                    Bytes.utf8("bucket"),
                    Bytes.utf8("object"),
                    Bytes.utf8("version"),
                    null,
                    100,
                    secondBody.payloadSha256(),
                    secondReservation.expiryEpochMs(),
                    proofKeys.getPrivate());
            final var badProof = CanonicalPayloadCommitProof.signed(
                    secondReservation.reservationId(),
                    scope.tenantScope(),
                    scope.shard().routeIncarnation().bytes(),
                    scope.shard().partition(),
                    secondMessage,
                    secondBody.objectStoreProfile(),
                    trustSet.version(),
                    1,
                    Bytes.utf8("bucket"),
                    Bytes.utf8("object"),
                    Bytes.utf8("version"),
                    null,
                    100,
                    secondBody.payloadSha256(),
                    secondReservation.expiryEpochMs(),
                    wrongProofKeys.getPrivate());
            final var invalidAt = source(
                    secondPrepareAt, secondPrepareAt.offset() + 1, secondPrepareAt.brokerLogAppendTimeEpochMs() + 1);
            assertEquals(
                    StableCode.PAYLOAD_PROOF_KEY_NOT_AUTHORIZED_AT_SOURCE_POSITION,
                    apply(loop, entries, commit(badProof, invalidAt, 710), invalidAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertNull(store.get(ColumnFamily.ID, TargetKeyCodec.message(secondMessage)));
            assertArrayEquals(
                    secondReservation.canonicalBytes(),
                    TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.ID, secondKey), TargetReservationRecord.VALUE_TYPE)
                            .payload());
            final var commitAt = source(invalidAt, invalidAt.offset() + 1, invalidAt.brokerLogAppendTimeEpochMs() + 1);
            final var commitCommand = commit(proof, commitAt, 720);
            assertEquals(
                    StableCode.SCHEDULED,
                    apply(loop, entries, commitCommand, commitAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            final var committedReservation = TargetReservationRecord.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.ID, secondKey), TargetReservationRecord.VALUE_TYPE)
                    .payload());
            assertEquals(PayloadReservationStatus.COMMITTED, committedReservation.status());
            assertEquals(2, committedReservation.stateVersion());
            assertEquals(secondReservation.prepareAnchor(), committedReservation.prepareAnchor());
            assertArrayEquals(
                    committedReservation.canonicalBytes(),
                    TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.ID, committedReservation.lookupKey()),
                                    TargetReservationRecord.VALUE_TYPE)
                            .payload());
            assertNull(store.get(ColumnFamily.TIMELINE, secondReservation.expiryKey()));
            final var committedMessage = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.ID, TargetKeyCodec.message(secondMessage)),
                            TargetMessageRecord.VALUE_TYPE)
                    .payload());
            assertEquals(commitAt, committedMessage.scheduleSource());
            assertEquals(1, committedMessage.stateVersion());
            assertEquals(committedReservation.committedPayload(), committedMessage.payloadReference());
            final var activeOwnerKey =
                    Bytes.concat(new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, 1}, secondMessage.bytes());
            final var activeOwner = TargetQuotaPayloadOwner.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.META, activeOwnerKey), TargetQuotaPayloadOwner.VALUE_TYPE)
                    .payload());
            committedReservation.requireOwner(activeOwner);
            assertEquals(TargetQuotaPayloadOwner.Phase.ACTIVE, activeOwner.phase());
            final var committedUsage = backend.prepareRead(
                            budget(), reader -> reader.aggregate().usage())
                    .value();
            assertEquals(0, committedUsage.resources().amount(CapacityDimension.RESERVATION_MESSAGES));
            assertEquals(0, committedUsage.resources().amount(CapacityDimension.RESERVATION_PAYLOAD_BYTES));
            assertEquals(2, committedUsage.resources().amount(CapacityDimension.ACTIVE_MESSAGES));
            final long afterCommit = store.latestSequenceNumber();
            final int resolvedProofs = proofResolutions.get();
            assertEquals(
                    StableCode.SCHEDULED,
                    apply(loop, entries, commitCommand, commitAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertEquals(afterCommit, store.latestSequenceNumber());
            assertEquals(resolvedProofs, proofResolutions.get());
            final var historicalAt = source(commitAt, commitAt.offset() + 1, secondReservation.expiryEpochMs() + 1);
            assertEquals(
                    StableCode.ALREADY_COMMITTED,
                    apply(loop, entries, commit(proof, historicalAt, 730), historicalAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertArrayEquals(
                    committedReservation.canonicalBytes(),
                    TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.ID, secondKey), TargetReservationRecord.VALUE_TYPE)
                            .payload());
            final var conflictProof = CanonicalPayloadCommitProof.signed(
                    secondReservation.reservationId(),
                    scope.tenantScope(),
                    scope.shard().routeIncarnation().bytes(),
                    scope.shard().partition(),
                    secondMessage,
                    secondBody.objectStoreProfile(),
                    trustSet.version(),
                    1,
                    Bytes.utf8("bucket"),
                    Bytes.utf8("different-object"),
                    Bytes.utf8("version"),
                    null,
                    100,
                    secondBody.payloadSha256(),
                    secondReservation.expiryEpochMs(),
                    proofKeys.getPrivate());
            final var conflictCommitAt =
                    source(historicalAt, historicalAt.offset() + 1, historicalAt.brokerLogAppendTimeEpochMs() + 1);
            final int beforeConflictProofs = proofResolutions.get();
            assertEquals(
                    StableCode.PAYLOAD_COMMIT_CONFLICT,
                    apply(loop, entries, commit(conflictProof, conflictCommitAt, 740), conflictCommitAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertEquals(beforeConflictProofs, proofResolutions.get());
            final var committedCancelAt = source(
                    conflictCommitAt, conflictCommitAt.offset() + 1, conflictCommitAt.brokerLogAppendTimeEpochMs() + 1);
            assertEquals(
                    StableCode.CANCELED,
                    apply(loop, entries, cancel(secondMessage, committedCancelAt, 750), committedCancelAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            final var retainedCommitAt = source(
                    committedCancelAt,
                    committedCancelAt.offset() + 1,
                    committedCancelAt.brokerLogAppendTimeEpochMs() + 1);
            assertEquals(
                    StableCode.ALREADY_COMMITTED,
                    apply(loop, entries, commit(proof, retainedCommitAt, 760), retainedCommitAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertArrayEquals(
                    committedReservation.canonicalBytes(),
                    TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.ID, secondKey), TargetReservationRecord.VALUE_TYPE)
                            .payload());
            final var finalOwner = TargetQuotaPayloadOwner.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.META, activeOwnerKey), TargetQuotaPayloadOwner.VALUE_TYPE)
                    .payload());
            committedReservation.requireOwner(finalOwner);
            assertEquals(TargetQuotaPayloadOwner.Phase.RETAINED, finalOwner.phase());
            final long beforeRetainedQuery = store.latestSequenceNumber();
            final var retainedSnapshot = queries.complete(
                            queries.prepare(budget(), secondReservation.reservationId(), (a, b) -> guard()),
                            (a, b) -> guard())
                    .orElseThrow();
            assertEquals(
                    PayloadReservationStatus.COMMITTED,
                    retainedSnapshot.reservation().status());
            assertEquals(TargetQuotaPayloadOwner.Phase.RETAINED, retainedSnapshot.payloadPhase());
            assertEquals(retainedCommitAt, retainedSnapshot.readSource());
            assertEquals(secondPrepareReceipt, retainedSnapshot.receipt(location));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> retainedSnapshot.receipt(new TargetReservationQueryStore.ReceiptLocation(
                            location.profile(), Bytes.utf8("bucket"), Bytes.utf8("wrong-object"))));
            assertEquals(beforeRetainedQuery, store.latestSequenceNumber());
            final var thirdAt = source(
                    retainedCommitAt, retainedCommitAt.offset() + 1, retainedCommitAt.brokerLogAppendTimeEpochMs() + 1);
            final var thirdMessage = new DelayMessageId(
                    cancel(secondMessage, thirdAt, 1800).commandId().bytes());
            final var thirdBody = new PrepareLargeScheduleBody(
                    thirdMessage,
                    thirdAt.brokerLogAppendTimeEpochMs() + 1000,
                    prepareIntent,
                    100,
                    bytes(32, 0xd1),
                    500,
                    trustSet.ref(),
                    modelPrepare.objectStoreProfile());
            final var thirdId = cancel(thirdMessage, thirdAt, 800).commandId();
            final var third = new PreparedCommand(
                    scope.shard(),
                    thirdId,
                    thirdMessage,
                    CommandType.PREPARE_LARGE_SCHEDULE,
                    fresh.protocolTuple(),
                    thirdBody.retryUntilEpochMs(),
                    thirdBody.canonicalBytes(),
                    CommandHash.compute(
                            fresh.protocolTuple(),
                            CommandType.PREPARE_LARGE_SCHEDULE,
                            thirdId,
                            thirdMessage,
                            thirdBody.retryUntilEpochMs(),
                            thirdBody.canonicalBytes()));
            assertEquals(
                    StableCode.OK,
                    apply(loop, entries, third, thirdAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            final byte[] thirdKey = Bytes.concat(new byte[] {TargetKeyCodec.RESERVATION_TAG, 1}, thirdMessage.bytes());
            final byte[] thirdRaw = store.get(ColumnFamily.ID, thirdKey);
            final var thirdReservation = TargetReservationRecord.decode(
                    TargetValueEnvelope.decode(thirdRaw, TargetReservationRecord.VALUE_TYPE)
                            .payload());
            final byte[] thirdOwnerKey =
                    Bytes.concat(new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, 1}, thirdMessage.bytes());
            final byte[] thirdOwnerRaw = store.get(ColumnFamily.META, thirdOwnerKey);
            final var thirdSnapshot = queries.read(budget(), thirdReservation.reservationId(), (a, b) -> guard())
                    .orElseThrow();
            final var thirdReceipt = thirdSnapshot.receipt(location);
            final var beforeFenceAt = source(thirdAt, thirdAt.offset() + 1, thirdAt.brokerLogAppendTimeEpochMs() + 1);
            applyFence(loop, entries, thirdReservation.expiryEpochMs() - 1, beforeFenceAt, keys);
            assertEquals(
                    PayloadReservationStatus.RESERVED,
                    queries.read(budget(), thirdReservation.reservationId(), (a, b) -> guard())
                            .orElseThrow()
                            .effectiveStatus());
            final var expiryStore = new TargetReservationExpiryStore(backend, scope, lineage, 1, reservationControls);
            assertTrue(expiryStore
                    .discover(budget(), null, (a, b) -> guard())
                    .candidate()
                    .isEmpty());
            final var staleBeforeFence = queries.prepare(budget(), thirdReservation.reservationId(), (a, b) -> guard());
            final var fenceAt =
                    source(beforeFenceAt, beforeFenceAt.offset() + 1, beforeFenceAt.brokerLogAppendTimeEpochMs() + 1);
            applyFence(loop, entries, thirdReservation.expiryEpochMs(), fenceAt, keys);
            assertThrows(IllegalStateException.class, () -> queries.complete(staleBeforeFence, (a, b) -> guard()));
            final long beforeOverlayQuery = store.latestSequenceNumber();
            final var expiredQuery = queryExecutor.submit(new TargetReservationQueryWorkClassExecutor.Request(
                    scope.shard(), bytes(16, 0xe6), thirdReservation.reservationId()));
            workerClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            assertEquals(
                    TargetReservationQueryWorkClassExecutor.Kind.COMPLETED,
                    expiredQuery.result().orElseThrow().kind());
            final var expiredSnapshot =
                    expiredQuery.result().orElseThrow().snapshot().orElseThrow();
            assertEquals(PayloadReservationStatus.EXPIRED, expiredSnapshot.effectiveStatus());
            assertEquals(
                    PayloadReservationStatus.RESERVED,
                    expiredSnapshot.reservation().status());
            assertEquals(1, expiredSnapshot.reservation().stateVersion());
            assertEquals(thirdAt, expiredSnapshot.reservation().mutation().source());
            assertEquals(fenceAt, expiredSnapshot.readSource());
            assertEquals(thirdReservation.expiryEpochMs(), expiredSnapshot.closedIngressDeadlineThrough());
            assertEquals(TargetQuotaPayloadOwner.Phase.RESERVED, expiredSnapshot.payloadPhase());
            assertEquals(thirdReceipt, expiredSnapshot.receipt(location));
            assertEquals(PayloadReservationStatus.RESERVED, thirdSnapshot.effectiveStatus());
            assertEquals(beforeOverlayQuery, store.latestSequenceNumber());
            assertEquals(
                    PayloadReservationStatus.ABANDONED,
                    queries.read(budget(), reserved.reservationId(), (a, b) -> guard())
                            .orElseThrow()
                            .effectiveStatus());
            assertEquals(
                    PayloadReservationStatus.COMMITTED,
                    queries.read(budget(), secondReservation.reservationId(), (a, b) -> guard())
                            .orElseThrow()
                            .effectiveStatus());
            final var thirdProof = CanonicalPayloadCommitProof.signed(
                    thirdReservation.reservationId(),
                    scope.tenantScope(),
                    scope.shard().routeIncarnation().bytes(),
                    scope.shard().partition(),
                    thirdMessage,
                    thirdBody.objectStoreProfile(),
                    trustSet.version(),
                    1,
                    Bytes.utf8("bucket"),
                    Bytes.utf8("object"),
                    Bytes.utf8("version"),
                    null,
                    100,
                    thirdBody.payloadSha256(),
                    thirdReservation.expiryEpochMs(),
                    proofKeys.getPrivate());
            final int proofCallsBeforeExpired = proofResolutions.get();
            final var expiredCommitAt = source(fenceAt, fenceAt.offset() + 1, fenceAt.brokerLogAppendTimeEpochMs() + 1);
            assertTrue(expiredCommitAt.brokerLogAppendTimeEpochMs() < thirdReservation.expiryEpochMs());
            final var expiredCommit = commit(thirdProof, expiredCommitAt, 810);
            assertEquals(
                    StableCode.RESERVATION_EXPIRED,
                    apply(loop, entries, expiredCommit, expiredCommitAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertEquals(proofCallsBeforeExpired, proofResolutions.get());
            final long beforeExpiredReplay = store.latestSequenceNumber();
            assertEquals(
                    StableCode.RESERVATION_EXPIRED,
                    apply(loop, entries, expiredCommit, expiredCommitAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertEquals(beforeExpiredReplay, store.latestSequenceNumber());
            final var expiredCancelAt = source(
                    expiredCommitAt, expiredCommitAt.offset() + 1, expiredCommitAt.brokerLogAppendTimeEpochMs() + 1);
            assertEquals(
                    StableCode.RESERVATION_EXPIRED,
                    apply(loop, entries, cancel(thirdMessage, expiredCancelAt, 811), expiredCancelAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            final var expiredRescheduleAt = source(
                    expiredCancelAt, expiredCancelAt.offset() + 1, expiredCancelAt.brokerLogAppendTimeEpochMs() + 1);
            assertEquals(
                    StableCode.RESERVATION_EXPIRED,
                    apply(
                                    loop,
                                    entries,
                                    reschedule(thirdMessage, expiredRescheduleAt, 812, new MessagePrecondition(0L, 1L)),
                                    expiredRescheduleAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            final var staleVersionAt = source(
                    expiredRescheduleAt,
                    expiredRescheduleAt.offset() + 1,
                    expiredRescheduleAt.brokerLogAppendTimeEpochMs() + 1);
            assertEquals(
                    StableCode.VERSION_CONFLICT,
                    apply(
                                    loop,
                                    entries,
                                    reschedule(thirdMessage, staleVersionAt, 813, new MessagePrecondition(0L, 2L)),
                                    staleVersionAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertArrayEquals(thirdRaw, store.get(ColumnFamily.ID, thirdKey));
            assertArrayEquals(thirdRaw, store.get(ColumnFamily.ID, thirdReservation.lookupKey()));
            assertArrayEquals(thirdRaw, store.get(ColumnFamily.TIMELINE, thirdReservation.expiryKey()));
            assertArrayEquals(thirdOwnerRaw, store.get(ColumnFamily.META, thirdOwnerKey));
            assertNull(store.get(ColumnFamily.ID, TargetKeyCodec.message(thirdMessage)));
            final var fencedUsage = backend.prepareRead(
                            budget(), reader -> reader.aggregate().usage())
                    .value();
            assertEquals(1, fencedUsage.resources().amount(CapacityDimension.RESERVATION_MESSAGES));
            assertEquals(100, fencedUsage.resources().amount(CapacityDimension.RESERVATION_PAYLOAD_BYTES));
            final var historicalAfterFenceAt = source(
                    staleVersionAt, staleVersionAt.offset() + 1, staleVersionAt.brokerLogAppendTimeEpochMs() + 1);
            assertEquals(
                    StableCode.ALREADY_COMMITTED,
                    apply(loop, entries, commit(proof, historicalAfterFenceAt, 814), historicalAfterFenceAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            // This binding-scope Close remains a historical authority fixture; the later global Close uses its writer.
            final var lateClosure = new TargetReservationControls.Closure(
                    thirdReservation.locator().target(),
                    thirdReservation.locator().scheduleBindingDigest(),
                    lineage,
                    historicalAfterFenceAt,
                    thirdReservation.expiryEpochMs());
            reservationClosures.put(Bytes.hex(lateClosure.bindingDigest()), lateClosure);
            final var afterLateClose = queries.read(budget(), thirdReservation.reservationId(), queryAuthority)
                    .orElseThrow();
            assertEquals(PayloadReservationStatus.EXPIRED, afterLateClose.effectiveStatus());
            assertEquals(lateClosure, afterLateClose.closure().orElseThrow());
            assertEquals(thirdReceipt, afterLateClose.receipt(location));
            final var badClosureQueries = new TargetReservationQueryStore(backend, scope, lineage, (reader, bound) -> {
                throw localReadExhaustion;
            });
            final var failedClosure = assertThrows(
                    IllegalStateException.class,
                    () -> badClosureQueries.read(budget(), thirdReservation.reservationId(), queryAuthority));
            assertEquals(localReadExhaustion, failedClosure.getCause());
            final var futureClosureQueries = new TargetReservationQueryStore(
                    backend,
                    scope,
                    lineage,
                    (reader, bound) -> java.util.Optional.of(new TargetReservationControls.Closure(
                            lateClosure.target(),
                            lateClosure.bindingDigest(),
                            lineage,
                            source(
                                    historicalAfterFenceAt,
                                    historicalAfterFenceAt.offset() + 1,
                                    historicalAfterFenceAt.brokerLogAppendTimeEpochMs() + 1),
                            lateClosure.fenceAtClose())));
            assertThrows(
                    IllegalStateException.class,
                    () -> futureClosureQueries.read(budget(), thirdReservation.reservationId(), queryAuthority));
            final var closureClock = new java.util.concurrent.atomic.AtomicLong();
            final var elapsedClosureQueries =
                    new TargetReservationQueryStore(backend, scope, lineage, (reader, bound) -> {
                        closureClock.set(10);
                        return java.util.Optional.empty();
                    });
            final var closureBudget = new BoundedReadBudget(2048, 100_000, 1, closureClock::get);
            assertThrows(
                    com.nereusstream.delay.store.ReadIncompleteException.class,
                    () -> elapsedClosureQueries.read(closureBudget, thirdReservation.reservationId(), queryAuthority));
            assertEquals(BoundedReadBudget.Exhaustion.ELAPSED, closureBudget.exhaustion());
            final var discovery = expiryStore.discover(budget(), null, (a, b) -> guard());
            assertArrayEquals(
                    thirdReservation.reservationId(),
                    discovery.candidate().orElseThrow().reservationId());
            assertTrue(expiryStore
                    .discover(budget(), discovery.nextCursor(), (a, b) -> guard())
                    .candidate()
                    .isEmpty());
            assertThrows(IllegalArgumentException.class, () -> new TargetReservationExpiryStore(
                            backend, scope, lineage, 1, reservationControls)
                    .discover(budget(), discovery.nextCursor(), (a, b) -> guard()));
            final long sourceSequenceBeforeExpiry = store.shardMutationSequence();
            final byte[] sourceBeforeExpiry =
                    store.get(ColumnFamily.META, com.nereusstream.delay.store.KeyCodec.metaFixed(3));
            final long nativeBeforeExpiry = store.latestSequenceNumber();
            assertThrows(
                    IllegalStateException.class,
                    () -> expiryStore.materialize(
                            budget(),
                            thirdReservation.reservationId(),
                            (a, b) -> guard(),
                            (a, b, c) -> {
                                throw new IllegalStateException("expiry physical capacity unavailable");
                            },
                            closureChange -> {}));
            assertEquals(nativeBeforeExpiry, store.latestSequenceNumber());
            assertArrayEquals(thirdRaw, store.get(ColumnFamily.ID, thirdKey));
            assertArrayEquals(thirdOwnerRaw, store.get(ColumnFamily.META, thirdOwnerKey));
            final var externalIncomplete = assertThrows(
                    IllegalStateException.class,
                    () -> expiryStore.materialize(
                            budget(),
                            thirdReservation.reservationId(),
                            (a, b) -> guard(),
                            (a, b, c) -> guard(),
                            delta -> {
                                throw localReadExhaustion;
                            }));
            assertEquals(localReadExhaustion, externalIncomplete.getCause());
            assertEquals(nativeBeforeExpiry, store.latestSequenceNumber());
            final var expiryCounters = new java.util.concurrent.atomic.AtomicReference<TargetQuotaDelta>();
            final var gcLimits = new TargetReservationExpiryWorkClassExecutor.Limits(2048, 100_000, 60_000_000_000L);
            final var gcWriteFails = new java.util.concurrent.atomic.AtomicBoolean(true);
            final var gcClockReads = new java.util.concurrent.atomic.AtomicInteger();
            final var gcExecutor = new TargetReservationExpiryWorkClassExecutor(
                    workerClasses,
                    expiryStore,
                    gcLimits,
                    () -> {},
                    queryAuthority,
                    (a, b, c) -> {
                        if (gcWriteFails.get()) {
                            throw localReadExhaustion;
                        }
                        return guard();
                    },
                    expiryCounters::set,
                    () -> {
                        gcClockReads.incrementAndGet();
                        return System.nanoTime();
                    });
            final int guardsBeforeGc = queryGuards.get();
            final var lostGc = gcExecutor.submit(
                    new TargetReservationExpiryWorkClassExecutor.Request(scope.shard(), bytes(16, 0xb1)));
            assertEquals(WorkClass.GC, lostGc.task().workClass());
            assertTrue(lostGc.task().bytes() > gcLimits.maximumBytes());
            assertEquals(guardsBeforeGc, queryGuards.get());
            assertEquals(0, gcClockReads.get());
            assertEquals(nativeBeforeExpiry, store.latestSequenceNumber());
            assertThrows(
                    IllegalStateException.class,
                    () -> gcExecutor.submit(
                            new TargetReservationExpiryWorkClassExecutor.Request(scope.shard(), bytes(16, 0xb2))));
            assertEquals(0, gcClockReads.get());
            queryOwnerCurrent.set(false);
            workerClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            assertEquals(
                    TargetReservationExpiryWorkClassExecutor.Kind.FAILED,
                    lostGc.result().orElseThrow().kind());
            assertEquals(nativeBeforeExpiry, store.latestSequenceNumber());
            queryOwnerCurrent.set(true);
            final var limitedGcExecutor = new TargetReservationExpiryWorkClassExecutor(
                    workerClasses,
                    expiryStore,
                    new TargetReservationExpiryWorkClassExecutor.Limits(2, 100_000, 60_000_000_000L),
                    () -> {},
                    queryAuthority,
                    (a, b, c) -> {
                        throw new AssertionError("exhausted shared discovery/materialization budget wrote");
                    },
                    expiryCounters::set,
                    System::nanoTime);
            final var limitedGc = limitedGcExecutor.submit(
                    new TargetReservationExpiryWorkClassExecutor.Request(scope.shard(), bytes(16, 0xb3)));
            workerClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            assertEquals(
                    TargetReservationExpiryWorkClassExecutor.Kind.READ_INCOMPLETE,
                    limitedGc.result().orElseThrow().kind());
            assertEquals(nativeBeforeExpiry, store.latestSequenceNumber());
            final var failedGc = gcExecutor.submit(
                    new TargetReservationExpiryWorkClassExecutor.Request(scope.shard(), bytes(16, 0xb4)));
            workerClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            assertEquals(
                    TargetReservationExpiryWorkClassExecutor.Kind.FAILED,
                    failedGc.result().orElseThrow().kind());
            assertEquals(
                    localReadExhaustion,
                    failedGc.result().orElseThrow().failure().getCause());
            assertEquals(nativeBeforeExpiry, store.latestSequenceNumber());
            gcWriteFails.set(false);
            final var successfulGc = gcExecutor.submit(
                    new TargetReservationExpiryWorkClassExecutor.Request(scope.shard(), bytes(16, 0xb5)));
            workerClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            assertEquals(
                    TargetReservationExpiryWorkClassExecutor.Kind.MATERIALIZED,
                    successfulGc.result().orElseThrow().kind());
            assertThrows(IllegalStateException.class, () -> workerClasses.retry(successfulGc.task()));
            assertEquals(0, workerClasses.registeredActions());
            assertEquals(sourceSequenceBeforeExpiry, store.shardMutationSequence());
            assertArrayEquals(
                    sourceBeforeExpiry,
                    store.get(ColumnFamily.META, com.nereusstream.delay.store.KeyCodec.metaFixed(3)));
            assertEquals(9, store.latestSequenceNumber() - nativeBeforeExpiry);
            final var materialized = queries.read(budget(), thirdReservation.reservationId(), (a, b) -> guard())
                    .orElseThrow();
            assertEquals(
                    PayloadReservationStatus.EXPIRED, materialized.reservation().status());
            assertEquals(PayloadReservationStatus.EXPIRED, materialized.effectiveStatus());
            assertEquals(2, materialized.reservation().stateVersion());
            assertEquals(thirdReceipt, materialized.receipt(location));
            assertEquals(TargetQuotaPayloadOwner.Phase.RETAINED, materialized.payloadPhase());
            assertEquals(historicalAfterFenceAt, materialized.readSource());
            final var expiryStamp = materialized.reservation().mutation();
            assertTrue(expiryStamp.reservationExpiry());
            assertTrue(expiryStamp.isLocalMutation());
            assertEquals(false, expiryStamp.isLocalClaim());
            assertEquals(1, expiryStamp.localOrdinal());
            assertEquals(sourceSequenceBeforeExpiry, expiryStamp.sequence());
            assertEquals(historicalAfterFenceAt, expiryStamp.source());
            assertEquals(
                    expiryStamp,
                    com.nereusstream.delay.protocol.TargetQuotaMutation.decode(expiryStamp.canonicalBytes()));
            assertThrows(IllegalArgumentException.class, expiryStamp::requireSourceApplied);
            final var laterClaimStamp = new com.nereusstream.delay.protocol.TargetQuotaMutation(
                    expiryStamp.sequence(), expiryStamp.source(), bytes(32, 0xf1), 2);
            laterClaimStamp.requireStoreSuccessorOf(expiryStamp);
            assertTrue(laterClaimStamp.isLocalClaim());
            final var sameOrdinalClaim = new com.nereusstream.delay.protocol.TargetQuotaMutation(
                    expiryStamp.sequence(),
                    expiryStamp.source(),
                    expiryStamp.mutationDigest(),
                    expiryStamp.localOrdinal());
            assertThrows(IllegalStateException.class, () -> sameOrdinalClaim.requireAtOrBefore(expiryStamp));

            assertThrows(IllegalStateException.class, () -> expiryStamp.requireStoreSuccessorOf(laterClaimStamp));
            assertNull(store.get(ColumnFamily.TIMELINE, thirdReservation.expiryKey()));
            assertArrayEquals(
                    store.get(ColumnFamily.ID, thirdKey), store.get(ColumnFamily.ID, thirdReservation.lookupKey()));
            final var materializedOwner = TargetQuotaPayloadOwner.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.META, thirdOwnerKey), TargetQuotaPayloadOwner.VALUE_TYPE)
                    .payload());
            materialized.reservation().requireOwner(materializedOwner);
            assertEquals(expiryStamp, materializedOwner.mutation());
            final var delta = expiryCounters.get();
            assertEquals(2, delta.changes().size());
            assertEquals(expiryStamp, delta.mutation());
            assertEquals(
                    fencedUsage.resources().amount(CapacityDimension.RETAINED_BYTES) + 100,
                    delta.nextAggregate().usage().resources().amount(CapacityDimension.RETAINED_BYTES));
            assertEquals(0, delta.nextAggregate().usage().resources().amount(CapacityDimension.RESERVATION_MESSAGES));
            assertEquals(
                    0, delta.nextAggregate().usage().resources().amount(CapacityDimension.RESERVATION_PAYLOAD_BYTES));
            final long nativeAfterExpiry = store.latestSequenceNumber();
            assertEquals(
                    false,
                    expiryStore.materialize(
                            budget(),
                            thirdReservation.reservationId(),
                            (a, b) -> guard(),
                            (a, b, c) -> {
                                throw new AssertionError("repeat materialization wrote");
                            },
                            ignored -> {
                                throw new AssertionError("repeat materialization reauthorized quota");
                            }));
            assertEquals(nativeAfterExpiry, store.latestSequenceNumber());
            final var afterMaterializationAt = source(
                    historicalAfterFenceAt,
                    historicalAfterFenceAt.offset() + 1,
                    historicalAfterFenceAt.brokerLogAppendTimeEpochMs() + 1);
            assertEquals(
                    StableCode.RESERVATION_EXPIRED,
                    apply(
                                    loop,
                                    entries,
                                    reschedule(
                                            thirdMessage, afterMaterializationAt, 815, new MessagePrecondition(0L, 2L)),
                                    afterMaterializationAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertEquals(sourceSequenceBeforeExpiry + 1, store.shardMutationSequence());
            // A source Prepare can insert behind the completed candidate while a sweep cursor is retained.
            final var fourthAt = source(
                    afterMaterializationAt,
                    afterMaterializationAt.offset() + 1,
                    afterMaterializationAt.brokerLogAppendTimeEpochMs() + 1);
            final var fourthMessage = new DelayMessageId(
                    cancel(thirdMessage, fourthAt, 1900).commandId().bytes());
            final var fourthBody = new PrepareLargeScheduleBody(
                    fourthMessage,
                    fourthAt.brokerLogAppendTimeEpochMs() + 1000,
                    prepareIntent,
                    100,
                    bytes(32, 0xd2),
                    100,
                    trustSet.ref(),
                    modelPrepare.objectStoreProfile());
            final var fourthId = cancel(fourthMessage, fourthAt, 900).commandId();
            final var fourth = new PreparedCommand(
                    scope.shard(),
                    fourthId,
                    fourthMessage,
                    CommandType.PREPARE_LARGE_SCHEDULE,
                    fresh.protocolTuple(),
                    fourthBody.retryUntilEpochMs(),
                    fourthBody.canonicalBytes(),
                    CommandHash.compute(
                            fresh.protocolTuple(),
                            CommandType.PREPARE_LARGE_SCHEDULE,
                            fourthId,
                            fourthMessage,
                            fourthBody.retryUntilEpochMs(),
                            fourthBody.canonicalBytes()));
            assertEquals(
                    StableCode.OK,
                    apply(loop, entries, fourth, fourthAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            final var fourthReservation = TargetReservationRecord.decode(TargetValueEnvelope.decode(
                            store.get(
                                    ColumnFamily.ID,
                                    Bytes.concat(
                                            new byte[] {TargetKeyCodec.RESERVATION_TAG, 1}, fourthMessage.bytes())),
                            TargetReservationRecord.VALUE_TYPE)
                    .payload());
            assertTrue(Arrays.compareUnsigned(fourthReservation.expiryKey(), thirdReservation.expiryKey()) < 0);
            final long beforeSweepEnd = store.latestSequenceNumber();
            final var endGc = gcExecutor.submit(
                    new TargetReservationExpiryWorkClassExecutor.Request(scope.shard(), bytes(16, 0xb6)));
            workerClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            assertEquals(
                    TargetReservationExpiryWorkClassExecutor.Kind.SWEEP_COMPLETE,
                    endGc.result().orElseThrow().kind());
            assertEquals(beforeSweepEnd, store.latestSequenceNumber());
            final var restartGc = gcExecutor.submit(
                    new TargetReservationExpiryWorkClassExecutor.Request(scope.shard(), bytes(16, 0xb7)));
            workerClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            assertEquals(
                    TargetReservationExpiryWorkClassExecutor.Kind.MATERIALIZED,
                    restartGc.result().orElseThrow().kind());
            assertEquals(9, store.latestSequenceNumber() - beforeSweepEnd);
            assertEquals(
                    PayloadReservationStatus.EXPIRED,
                    queries.read(budget(), fourthReservation.reservationId(), queryAuthority)
                            .orElseThrow()
                            .reservation()
                            .status());
            assertEquals(fourthAt, store.appliedShardLogPosition());
            assertEquals(0, workerClasses.registeredActions());
            final var fifthAt = source(fourthAt, fourthAt.offset() + 1, fourthAt.brokerLogAppendTimeEpochMs() + 1);
            final var fifthMessage = new DelayMessageId(
                    cancel(fourthMessage, fifthAt, 2000).commandId().bytes());
            final var fifthBody = new PrepareLargeScheduleBody(
                    fifthMessage,
                    fifthAt.brokerLogAppendTimeEpochMs() + 1000,
                    prepareIntent,
                    100,
                    bytes(32, 0xd3),
                    1000,
                    trustSet.ref(),
                    modelPrepare.objectStoreProfile());
            final var fifthId = cancel(fifthMessage, fifthAt, 1000).commandId();
            final var fifth = new PreparedCommand(
                    scope.shard(),
                    fifthId,
                    fifthMessage,
                    CommandType.PREPARE_LARGE_SCHEDULE,
                    fresh.protocolTuple(),
                    fifthBody.retryUntilEpochMs(),
                    fifthBody.canonicalBytes(),
                    CommandHash.compute(
                            fresh.protocolTuple(),
                            CommandType.PREPARE_LARGE_SCHEDULE,
                            fifthId,
                            fifthMessage,
                            fifthBody.retryUntilEpochMs(),
                            fifthBody.canonicalBytes()));
            assertEquals(
                    StableCode.OK,
                    apply(loop, entries, fifth, fifthAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            final byte[] fifthKey = Bytes.concat(new byte[] {TargetKeyCodec.RESERVATION_TAG, 1}, fifthMessage.bytes());
            final byte[] fifthRaw = store.get(ColumnFamily.ID, fifthKey);
            final var fifthReservation = TargetReservationRecord.decode(
                    TargetValueEnvelope.decode(fifthRaw, TargetReservationRecord.VALUE_TYPE)
                            .payload());
            final var preCloseSnapshot = queries.read(budget(), fifthReservation.reservationId(), queryAuthority)
                    .orElseThrow();
            final byte[] pendingKey = TargetKeyCodec.message(fresh.delayMessageId());
            final byte[] pendingRaw = store.get(ColumnFamily.ID, pendingKey);
            final var queueBeforeClose = TargetQueueState.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.META, TargetKeyCodec.state(physical.id())),
                            TargetQueueState.VALUE_TYPE)
                    .payload());
            final var pendingHead = queueBeforeClose.domains().getFirst().ordinaryHead();
            assertTrue(pendingHead != null);
            final byte[] pendingWork = store.get(ColumnFamily.TIMELINE, pendingHead.key());
            final var beforeCloseFenceAt =
                    source(fifthAt, fifthAt.offset() + 1, fifthAt.brokerLogAppendTimeEpochMs() + 1);
            applyFence(loop, entries, fifthReservation.expiryEpochMs() - 1, beforeCloseFenceAt, keys);
            final var closeAt = source(
                    beforeCloseFenceAt,
                    beforeCloseFenceAt.offset() + 1,
                    beforeCloseFenceAt.brokerLogAppendTimeEpochMs() + 1);
            final var closeRequest = new TargetCloseRequest(
                    physical.id(),
                    List.of(new TargetCloseRequest.ShardTarget(
                            scope.shard(),
                            queueBeforeClose.accountingIncarnation(),
                            queueBeforeClose.controlVersion())),
                    new CloseLaneRequest(
                            new ControlReason(ControlReasonKind.OPERATOR_REQUEST, null, null),
                            ClosePolicy._FREEZE_UNADMITTED_AND_PRESERVE_ADMITTED,
                            false,
                            AcknowledgementSet.empty()));
            final var signedClose = signedClose(
                    closeRequest, bytes(32, 0xca), actor, keys, closeAt.brokerLogAppendTimeEpochMs() + 2000);
            registrations.register(signedClose.control());
            final var closeBody = TargetCloseBody.decode(signedClose.mutation().canonicalBody());
            assertArrayEquals(closeBody.canonicalBytes(), signedClose.mutation().canonicalBody());
            assertEquals(closeRequest, TargetCloseRequest.decode(closeRequest.canonicalBytes()));
            final long beforeCloseNative = store.latestSequenceNumber();
            final var beforeCloseUsage = backend.prepareRead(
                            budget(), reader -> reader.aggregate().usage())
                    .value();
            final var rejectedClosePlan = closeStore.prepareFirst(
                    budget(), signedClose.control(), signedClose.mutation(), closeAt, closeAuthority);
            assertThrows(
                    IllegalStateException.class,
                    () -> closeStore.commit(rejectedClosePlan, (a, b, c) -> {
                        throw new IllegalStateException("Close physical capacity unavailable");
                    }));
            assertEquals(beforeCloseNative, store.latestSequenceNumber());
            assertNull(store.get(ColumnFamily.META, TargetKeyCodec.close(physical.id())));
            assertArrayEquals(pendingRaw, store.get(ColumnFamily.ID, pendingKey));
            assertArrayEquals(
                    queueBeforeClose.canonicalBytes(),
                    TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.META, TargetKeyCodec.state(physical.id())),
                                    TargetQueueState.VALUE_TYPE)
                            .payload());
            applyClose(loop, entries, signedClose, closeAt);
            assertEquals(13, store.latestSequenceNumber() - beforeCloseNative);
            assertEquals(1, closeResolutions.get());
            final byte[] markerRaw = store.get(ColumnFamily.META, TargetKeyCodec.close(physical.id()));
            final var durableClose = TargetCloseRecord.decodeForStore(
                    TargetKeyCodec.close(physical.id()),
                    TargetValueEnvelope.decode(markerRaw, TargetCloseRecord.VALUE_TYPE)
                            .payload(),
                    scope.shard(),
                    lineage);
            assertEquals(closeAt, durableClose.mutation().source());
            final var initialCursor = com.nereusstream.delay.protocol.TargetCloseCursorRecord.decodeForStore(
                    TargetKeyCodec.closeCursor(physical.id()),
                    TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.META, TargetKeyCodec.closeCursor(physical.id())),
                                    com.nereusstream.delay.protocol.TargetCloseCursorRecord.VALUE_TYPE)
                            .payload(),
                    durableClose,
                    lineage);
            assertEquals(1, initialCursor.revision());
            assertFalse(initialCursor.complete());
            assertNull(initialCursor.afterMessageId());
            assertEquals(fifthReservation.expiryEpochMs() - 1, durableClose.fenceAtClose());
            final var queueAfterClose = TargetQueueState.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.META, TargetKeyCodec.state(physical.id())),
                            TargetQueueState.VALUE_TYPE)
                    .payload());
            durableClose.requireQueue(queueAfterClose);
            assertEquals(TargetQueueState.AdmissionState.CLOSED, queueAfterClose.admissionState());
            assertEquals(queueBeforeClose.controlVersion() + 1, queueAfterClose.controlVersion());
            assertNull(queueAfterClose.domains().getFirst().ordinaryHead());
            assertNull(queueAfterClose.domains().getFirst().nativeHead());
            assertArrayEquals(pendingWork, store.get(ColumnFamily.TIMELINE, pendingHead.key()));
            assertArrayEquals(pendingRaw, store.get(ColumnFamily.ID, pendingKey));
            final var afterCloseUsage = backend.prepareRead(
                            budget(), reader -> reader.aggregate().usage())
                    .value();
            for (var dimension : List.of(
                    CapacityDimension.ACTIVE_MESSAGES,
                    CapacityDimension.RESERVATION_MESSAGES,
                    CapacityDimension.RESERVATION_PAYLOAD_BYTES,
                    CapacityDimension.RETAINED_BYTES)) {
                assertEquals(
                        beforeCloseUsage.resources().amount(dimension),
                        afterCloseUsage.resources().amount(dimension));
            }
            final long afterCloseNative = store.latestSequenceNumber();
            applyClose(loop, entries, signedClose, closeAt);
            assertEquals(afterCloseNative, store.latestSequenceNumber());
            assertEquals(1, closeResolutions.get());
            final var markerClock = new java.util.concurrent.atomic.AtomicLong();
            final var markerBudget = new BoundedReadBudget(2048, 100_000, 1, markerClock::get);
            final var boundedMarkerQueries = new TargetReservationQueryStore(
                    backend, scope, lineage, closeStore.reservationControls((reader, bound) -> {
                        markerClock.set(10);
                        return java.util.Optional.empty();
                    }));
            assertThrows(
                    com.nereusstream.delay.store.ReadIncompleteException.class,
                    () -> boundedMarkerQueries.read(markerBudget, fifthReservation.reservationId(), queryAuthority));
            assertEquals(BoundedReadBudget.Exhaustion.ELAPSED, markerBudget.exhaustion());
            final var failedScopeQueries = new TargetReservationQueryStore(
                    backend, scope, lineage, closeStore.reservationControls((reader, bound) -> {
                        throw localReadExhaustion;
                    }));
            assertEquals(
                    localReadExhaustion,
                    assertThrows(
                                    IllegalStateException.class,
                                    () -> failedScopeQueries.read(
                                            budget(), fifthReservation.reservationId(), queryAuthority))
                            .getCause());
            final var futureScopeQueries = new TargetReservationQueryStore(
                    backend,
                    scope,
                    lineage,
                    closeStore.reservationControls(
                            (reader, bound) -> java.util.Optional.of(new TargetReservationControls.Closure(
                                    bound.target(),
                                    bound.digest(),
                                    lineage,
                                    source(closeAt, closeAt.offset() + 1, closeAt.brokerLogAppendTimeEpochMs() + 1),
                                    durableClose.fenceAtClose()))));
            assertThrows(
                    IllegalStateException.class,
                    () -> futureScopeQueries.read(budget(), fifthReservation.reservationId(), queryAuthority));
            assertEquals(afterCloseNative, store.latestSequenceNumber());
            final var closedDiscovery =
                    new TargetReservationClosureStore(backend, scope, lineage, 1, reservationControls);
            assertArrayEquals(
                    fifthReservation.reservationId(),
                    closedDiscovery
                            .discover(budget(), physical.id(), queryAuthority)
                            .orElseThrow()
                            .reservationId());
            assertTrue(closedDiscovery
                    .discover(
                            budget(),
                            new com.nereusstream.delay.protocol.TargetPartitionId(bytes(32, 0xfe)),
                            queryAuthority)
                    .isEmpty());
            assertArrayEquals(fifthRaw, store.get(ColumnFamily.ID, fifthReservation.targetIndexKey()));
            final long beforeIncompleteClose = store.latestSequenceNumber();
            assertFalse(closedDiscovery.completeEmpty(
                    budget(),
                    physical.id(),
                    queryAuthority,
                    (a, b, c) -> {
                        throw new AssertionError("active reservation cannot complete Close");
                    },
                    cursorChange -> {
                        throw new AssertionError("active reservation cannot account Close completion");
                    }));
            assertEquals(beforeIncompleteClose, store.latestSequenceNumber());
            assertThrows(
                    com.nereusstream.delay.store.ReadIncompleteException.class,
                    () -> closedDiscovery.discover(
                            new BoundedReadBudget(2, 100_000, 60_000_000_000L, System::nanoTime),
                            physical.id(),
                            queryAuthority));
            assertArrayEquals(
                    new byte[] {TargetKeyCodec.TARGET_RESERVATION_TAG, 2},
                    TargetKeyCodec.targetReservationUpperBound(
                            new com.nereusstream.delay.protocol.TargetPartitionId(bytes(32, 0xff))));
            final var closeSnapshot = queries.read(budget(), fifthReservation.reservationId(), queryAuthority)
                    .orElseThrow();
            assertEquals(PayloadReservationStatus.ABANDONED, closeSnapshot.effectiveStatus());
            assertEquals(
                    PayloadReservationStatus.RESERVED,
                    closeSnapshot.reservation().status());
            assertEquals(TargetQuotaPayloadOwner.Phase.RESERVED, closeSnapshot.payloadPhase());
            assertEquals(preCloseSnapshot.receipt(location), closeSnapshot.receipt(location));
            final var closures = new TargetReservationClosureStore(backend, scope, lineage, 1, reservationControls);
            final long beforeClosureSourceSequence = store.shardMutationSequence();
            final byte[] beforeClosureSource =
                    store.get(ColumnFamily.META, com.nereusstream.delay.store.KeyCodec.metaFixed(3));
            final long beforeClosureWrite = store.latestSequenceNumber();
            assertThrows(
                    IllegalStateException.class,
                    () -> closures.materialize(
                            budget(),
                            fifthReservation.reservationId(),
                            queryAuthority,
                            (a, b, c) -> {
                                throw new IllegalStateException("closure capacity unavailable");
                            },
                            closureChange -> {}));
            assertEquals(beforeClosureWrite, store.latestSequenceNumber());
            assertArrayEquals(fifthRaw, store.get(ColumnFamily.ID, fifthKey));
            assertArrayEquals(fifthRaw, store.get(ColumnFamily.TIMELINE, fifthReservation.expiryKey()));
            final var closureDelta = new java.util.concurrent.atomic.AtomicReference<TargetQuotaDelta>();
            final var closeGc = new TargetReservationClosureWorkClassExecutor(
                    workerClasses,
                    backend,
                    scope,
                    lineage,
                    1,
                    reservationControls,
                    new TargetReservationClosureWorkClassExecutor.Limits(4096, 250_000, 60_000_000_000L),
                    () -> {},
                    queryAuthority,
                    (a, b, c) -> guard(),
                    closureDelta::set,
                    ignored -> {
                        throw new AssertionError("Close-first candidate used expiry accounting");
                    },
                    ignored -> {
                        throw new AssertionError("active reservation completed empty Close");
                    },
                    System::nanoTime);
            queryOwnerCurrent.set(false);
            final var rejectedCloseGc = closeGc.submit(new TargetReservationClosureWorkClassExecutor.Request(
                    scope.shard(), physical.id(), bytes(16, 0xd0)));
            workerClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            assertEquals(
                    TargetReservationClosureWorkClassExecutor.Kind.FAILED,
                    rejectedCloseGc.result().orElseThrow().kind());
            assertEquals(beforeClosureWrite, store.latestSequenceNumber());
            queryOwnerCurrent.set(true);
            boolean closeFoundBySweep = false;
            for (int scan = 0; scan < 2; scan++) {
                final var closeGcStep = closeGc.submit(new TargetReservationClosureWorkClassExecutor.SweepRequest(
                        scope.shard(), bytes(16, 0xe0 + scan)));
                assertEquals(WorkClass.GC, closeGcStep.task().workClass());
                assertEquals(beforeClosureWrite, store.latestSequenceNumber());
                workerClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
                final var kind = closeGcStep.result().orElseThrow().kind();
                if (kind == TargetReservationClosureWorkClassExecutor.Kind.CLOSE_MATERIALIZED) {
                    closeFoundBySweep = true;
                    break;
                }
                assertEquals(TargetReservationClosureWorkClassExecutor.Kind.SKIPPED_COMPLETE, kind);
            }
            assertTrue(closeFoundBySweep);
            assertEquals(10, store.latestSequenceNumber() - beforeClosureWrite);
            assertEquals(beforeClosureSourceSequence, store.shardMutationSequence());
            assertArrayEquals(
                    beforeClosureSource,
                    store.get(ColumnFamily.META, com.nereusstream.delay.store.KeyCodec.metaFixed(3)));
            assertEquals(closeAt, store.appliedShardLogPosition());
            final var closedDurable = queries.read(budget(), fifthReservation.reservationId(), queryAuthority)
                    .orElseThrow();
            assertEquals(
                    PayloadReservationStatus.ABANDONED,
                    closedDurable.reservation().status());
            assertEquals(closeSnapshot.closure(), closedDurable.closure());
            assertEquals(preCloseSnapshot.receipt(location), closedDurable.receipt(location));
            assertEquals(TargetQuotaPayloadOwner.Phase.RETAINED, closedDurable.payloadPhase());
            assertEquals(2, closedDurable.reservation().stateVersion());
            final var closureStamp = closedDurable.reservation().mutation();
            assertTrue(closureStamp.reservationClosure());
            assertFalse(closureStamp.reservationExpiry());
            assertFalse(closureStamp.isLocalClaim());
            assertEquals(1, closureStamp.localOrdinal());
            assertEquals(
                    closureStamp,
                    com.nereusstream.delay.protocol.TargetQuotaMutation.decode(closureStamp.canonicalBytes()));
            assertEquals(closureStamp, closureDelta.get().mutation());
            final var terminalCursor = com.nereusstream.delay.protocol.TargetCloseCursorRecord.decodeForStore(
                    TargetKeyCodec.closeCursor(physical.id()),
                    TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.META, TargetKeyCodec.closeCursor(physical.id())),
                                    com.nereusstream.delay.protocol.TargetCloseCursorRecord.VALUE_TYPE)
                            .payload(),
                    durableClose,
                    lineage);
            assertEquals(2, terminalCursor.revision());
            assertTrue(terminalCursor.complete());
            assertArrayEquals(fifthReservation.locator().messageId().bytes(), terminalCursor.afterMessageId());
            assertEquals(closureStamp, terminalCursor.mutation());
            assertThrows(IllegalArgumentException.class, closureStamp::requireSourceApplied);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new com.nereusstream.delay.protocol.TargetQuotaMutation(
                            closureStamp.sequence(),
                            closureStamp.source(),
                            closureStamp.mutationDigest(),
                            1,
                            true,
                            true));
            final var forgedExpiry = new com.nereusstream.delay.protocol.TargetQuotaMutation(
                    closureStamp.sequence(), closureStamp.source(), closureStamp.mutationDigest(), 1, true);
            assertThrows(IllegalStateException.class, () -> forgedExpiry.requireAtOrBefore(closureStamp));
            assertThrows(
                    IllegalStateException.class,
                    () -> fifthReservation.finishClosed(
                            forgedExpiry, closedDurable.closure().orElseThrow()));
            final byte[] closedReservationRaw = store.get(ColumnFamily.ID, fifthKey);
            assertArrayEquals(closedReservationRaw, store.get(ColumnFamily.ID, fifthReservation.lookupKey()));
            assertNull(store.get(ColumnFamily.TIMELINE, fifthReservation.expiryKey()));
            assertNull(store.get(ColumnFamily.ID, fifthReservation.targetIndexKey()));
            assertTrue(new TargetReservationClosureStore(backend, scope, lineage, 1, reservationControls)
                    .discover(budget(), physical.id(), queryAuthority)
                    .isEmpty());
            assertFalse(closures.completeEmpty(
                    budget(),
                    physical.id(),
                    queryAuthority,
                    (a, b, c) -> {
                        throw new AssertionError("completed Close cannot rewrite cursor");
                    },
                    cursorChange -> {
                        throw new AssertionError("completed Close cannot reaccount cursor");
                    }));
            final var afterClosureUsage = backend.prepareRead(
                            budget(), reader -> reader.aggregate().usage())
                    .value();
            assertEquals(
                    beforeCloseUsage.resources().amount(CapacityDimension.RESERVATION_MESSAGES) - 1,
                    afterClosureUsage.resources().amount(CapacityDimension.RESERVATION_MESSAGES));
            assertEquals(
                    beforeCloseUsage.resources().amount(CapacityDimension.RESERVATION_PAYLOAD_BYTES) - 100,
                    afterClosureUsage.resources().amount(CapacityDimension.RESERVATION_PAYLOAD_BYTES));
            assertEquals(
                    beforeCloseUsage.resources().amount(CapacityDimension.RETAINED_BYTES) + 100,
                    afterClosureUsage.resources().amount(CapacityDimension.RETAINED_BYTES));
            final long afterClosureWrite = store.latestSequenceNumber();
            final var repeatedCloseGc = closeGc.submit(new TargetReservationClosureWorkClassExecutor.Request(
                    scope.shard(), physical.id(), bytes(16, 0xd2)));
            workerClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            assertEquals(
                    TargetReservationClosureWorkClassExecutor.Kind.ALREADY_COMPLETE,
                    repeatedCloseGc.result().orElseThrow().kind());
            assertEquals(afterClosureWrite, store.latestSequenceNumber());
            final var missingCloseGc = closeGc.submit(new TargetReservationClosureWorkClassExecutor.Request(
                    scope.shard(),
                    new com.nereusstream.delay.protocol.TargetPartitionId(bytes(32, 0xfe)),
                    bytes(16, 0xd3)));
            workerClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            assertEquals(
                    TargetReservationClosureWorkClassExecutor.Kind.NO_CLOSE_MARKER,
                    missingCloseGc.result().orElseThrow().kind());
            assertEquals(afterClosureWrite, store.latestSequenceNumber());
            assertFalse(closures.materialize(
                    budget(),
                    fifthReservation.reservationId(),
                    queryAuthority,
                    (a, b, c) -> {
                        throw new AssertionError("terminal Close rewrote");
                    },
                    closureChange -> {
                        throw new AssertionError("terminal Close reaccounted");
                    }));
            assertEquals(afterClosureWrite, store.latestSequenceNumber());
            final var fifthFenceAt = source(closeAt, closeAt.offset() + 1, closeAt.brokerLogAppendTimeEpochMs() + 1);
            applyFence(loop, entries, fifthReservation.expiryEpochMs(), fifthFenceAt, keys);
            assertEquals(
                    PayloadReservationStatus.ABANDONED,
                    queries.read(budget(), fifthReservation.reservationId(), queryAuthority)
                            .orElseThrow()
                            .effectiveStatus());
            assertEquals(PayloadReservationStatus.RESERVED, preCloseSnapshot.effectiveStatus());
            assertEquals(PayloadReservationStatus.ABANDONED, closeSnapshot.effectiveStatus());
            final var closedRescheduleAt =
                    source(fifthFenceAt, fifthFenceAt.offset() + 1, fifthFenceAt.brokerLogAppendTimeEpochMs() + 1);
            final var closedReschedule =
                    reschedule(fifthMessage, closedRescheduleAt, 1100, new MessagePrecondition(0L, 2L));
            assertEquals(
                    StableCode.PAYLOAD_RESERVATION_CLOSED,
                    apply(loop, entries, closedReschedule, closedRescheduleAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            final var closedCancelAt = source(
                    closedRescheduleAt,
                    closedRescheduleAt.offset() + 1,
                    closedRescheduleAt.brokerLogAppendTimeEpochMs() + 1);
            assertEquals(
                    StableCode.PAYLOAD_RESERVATION_CLOSED,
                    apply(loop, entries, cancel(fifthMessage, closedCancelAt, 1101), closedCancelAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            final var fifthProof = CanonicalPayloadCommitProof.signed(
                    fifthReservation.reservationId(),
                    scope.tenantScope(),
                    scope.shard().routeIncarnation().bytes(),
                    scope.shard().partition(),
                    fifthMessage,
                    fifthBody.objectStoreProfile(),
                    trustSet.version(),
                    1,
                    Bytes.utf8("bucket"),
                    Bytes.utf8("object"),
                    Bytes.utf8("version"),
                    null,
                    100,
                    fifthBody.payloadSha256(),
                    fifthReservation.expiryEpochMs(),
                    proofKeys.getPrivate());
            final var closedCommitAt = source(
                    closedCancelAt, closedCancelAt.offset() + 1, closedCancelAt.brokerLogAppendTimeEpochMs() + 1);
            final int proofCallsBeforeClosed = proofResolutions.get();
            final var closedCommit = commit(fifthProof, closedCommitAt, 1102);
            assertEquals(
                    StableCode.PAYLOAD_RESERVATION_CLOSED,
                    apply(loop, entries, closedCommit, closedCommitAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertEquals(proofCallsBeforeClosed, proofResolutions.get());
            final long beforeClosedGc = store.latestSequenceNumber();
            assertEquals(
                    StableCode.PAYLOAD_RESERVATION_CLOSED,
                    apply(loop, entries, closedCommit, closedCommitAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            for (int n = 0; n < 4; n++) {
                final var gc = gcExecutor.submit(
                        new TargetReservationExpiryWorkClassExecutor.Request(scope.shard(), bytes(16, 0xc0 + n)));
                workerClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
                assertEquals(
                        TargetReservationExpiryWorkClassExecutor.Kind.SWEEP_COMPLETE,
                        gc.result().orElseThrow().kind());
            }
            assertEquals(beforeClosedGc, store.latestSequenceNumber());
            assertArrayEquals(closedReservationRaw, store.get(ColumnFamily.ID, fifthKey));
            assertNull(store.get(ColumnFamily.TIMELINE, fifthReservation.expiryKey()));
            assertEquals(
                    PayloadReservationStatus.EXPIRED,
                    queries.read(budget(), thirdReservation.reservationId(), queryAuthority)
                            .orElseThrow()
                            .effectiveStatus());
            assertEquals(0, workerClasses.registeredActions());

            final var pendingPhysical =
                    new CanonicalTargetPartition(physical.resource(), physical.physicalPartition() + 2);
            final var pendingTarget = pendingPhysical.id();
            reopenedCloseTargets[2] = pendingTarget;
            final var pendingGrantRequest = new TargetQuotaGrantControlRequest(
                    new TargetQuotaGrant(
                            scope.forTarget(pendingTarget),
                            bytes(32, 0x64),
                            1,
                            originalGrant.accounting(),
                            new TargetQuotaUsage(new CapacityVector(targetAmounts), 1, 64, 64, 64),
                            originalGrant.tenantPolicyVersion(),
                            originalGrant.tenantPolicyHash()),
                    null,
                    null);
            final var pendingGrantAt = source(
                    closedCommitAt, closedCommitAt.offset() + 1, closedCommitAt.brokerLogAppendTimeEpochMs() + 1);
            final long retryUntil =
                    Math.max(fifthReservation.expiryEpochMs(), pendingGrantAt.brokerLogAppendTimeEpochMs()) + 2000;
            final var pendingGrant = signed(pendingGrantRequest, bytes(32, 0x65), actor, keys, retryUntil);
            registrations.register(pendingGrant.control());
            assertEquals(
                    StableCode.OK,
                    grantStore
                            .commit(
                                    grantStore.prepareFirst(
                                            budget(),
                                            pendingGrant.control(),
                                            pendingGrant.mutation(),
                                            pendingGrantAt,
                                            authority(
                                                    registrations,
                                                    keys,
                                                    actor,
                                                    pendingGrantAt,
                                                    pendingGrantRequest,
                                                    (a, b, c, d) -> {})),
                                    (a, b, c) -> guard())
                            .stableCode());
            final var pendingActivation = TargetQuotaGrantActivation.decode(TargetValueEnvelope.decode(
                            store.get(
                                    ColumnFamily.META,
                                    Bytes.concat(
                                            new byte[] {TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG, 1},
                                            pendingGrantRequest.next().scope().keySuffix())),
                            TargetQuotaGrantActivation.VALUE_TYPE)
                    .payload());
            final var pendingQueue = new TargetQueueState(
                    pendingTarget,
                    1,
                    1,
                    TargetQueueState.AdmissionState.OPEN,
                    pendingActivation.allocation().identity().accountingIncarnation(),
                    0,
                    List.of());
            final var pendingQueueAt = source(
                    pendingGrantAt, pendingGrantAt.offset() + 1, pendingGrantAt.brokerLogAppendTimeEpochMs() + 1);
            new TargetMessageStore(backend, 1, 1, 1)
                    .applyAccounted(
                            budget(),
                            reader -> new TargetMessageStore.Input(
                                    List.of(),
                                    List.of(),
                                    List.of(
                                            reader.replace(
                                                    ColumnFamily.META,
                                                    TargetKeyCodec.identity(pendingTarget),
                                                    CanonicalTargetPartition.VALUE_TYPE,
                                                    pendingPhysical.canonicalBytes()),
                                            reader.replace(
                                                    ColumnFamily.META,
                                                    TargetKeyCodec.state(pendingTarget),
                                                    TargetQueueState.VALUE_TYPE,
                                                    pendingQueue.canonicalBytes()))),
                            new TargetSourceAccounting(
                                    scope,
                                    lineage,
                                    pendingQueueAt,
                                    Bytes.sha256(Bytes.utf8("pending-close-target-queue-fixture")),
                                    16,
                                    1,
                                    1),
                            (a, b, c) -> guard());
            final var pendingCloseAt = source(
                    pendingQueueAt, pendingQueueAt.offset() + 1, pendingQueueAt.brokerLogAppendTimeEpochMs() + 1);
            final var pendingCloseRequest = new TargetCloseRequest(
                    pendingTarget,
                    List.of(new TargetCloseRequest.ShardTarget(
                            scope.shard(), pendingQueue.accountingIncarnation(), pendingQueue.controlVersion())),
                    new CloseLaneRequest(
                            new ControlReason(ControlReasonKind.OPERATOR_REQUEST, null, null),
                            ClosePolicy._FREEZE_UNADMITTED_AND_PRESERVE_ADMITTED,
                            false,
                            AcknowledgementSet.empty()));
            final var pendingSignedClose = signedClose(
                    pendingCloseRequest,
                    bytes(32, 0x66),
                    actor,
                    keys,
                    pendingCloseAt.brokerLogAppendTimeEpochMs() + 2000);
            registrations.register(pendingSignedClose.control());
            final var pendingCloseStore = new TargetCloseStore(backend, scope, lineage, 16, 1);
            assertEquals(
                    StableCode.OK,
                    pendingCloseStore
                            .commit(
                                    pendingCloseStore.prepareFirst(
                                            budget(),
                                            pendingSignedClose.control(),
                                            pendingSignedClose.mutation(),
                                            pendingCloseAt,
                                            new TargetCloseVerifier.Authority(
                                                    registrations,
                                                    (version, position) -> keys.getPublic(),
                                                    (actualScope, requestToClose, position, queueToClose) -> {
                                                        assertEquals(scope, actualScope);
                                                        assertEquals(pendingTarget, requestToClose.target());
                                                        assertEquals(
                                                                pendingQueue.controlVersion(),
                                                                queueToClose.controlVersion());
                                                    },
                                                    actor,
                                                    prepared -> true)),
                                    (a, b, c) -> guard())
                            .stableCode());
            assertEquals(
                    TargetReservationClosureStore.Progress.OPEN,
                    new TargetReservationClosureStore(
                                    backend,
                                    scope,
                                    lineage,
                                    1,
                                    pendingCloseStore.reservationControls(
                                            (reader, bound) -> java.util.Optional.empty()))
                            .progress(budget(), pendingTarget, (a, b) -> guard()));
            final var oldGc = runtime.newCloseGcExecutor(
                    workerClasses,
                    pendingCloseStore.reservationControls((reader, bound) -> java.util.Optional.empty()),
                    new TargetReservationClosureWorkClassExecutor.Limits(4096, 250_000, 60_000_000_000L),
                    (a, b, c) -> guard(),
                    ignored -> {
                        throw new AssertionError("old Owner cannot materialize Close");
                    },
                    ignored -> {
                        throw new AssertionError("old Owner cannot expire Close");
                    },
                    ignored -> {
                        throw new AssertionError("old Owner cannot complete Close");
                    },
                    () -> 100);
            final long beforeOldOwnerLoss = store.latestSequenceNumber();
            final var oldQueued = oldGc.submit(
                    new TargetReservationClosureWorkClassExecutor.SweepRequest(scope.shard(), bytes(16, 0xe7)));
            assertTrue(leases.release(active));
            workerClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            assertEquals(
                    TargetReservationClosureWorkClassExecutor.Kind.FAILED,
                    oldQueued.result().orElseThrow().kind());
            assertTrue(runtime.fenced());
            assertEquals(beforeOldOwnerLoss, store.latestSequenceNumber());
            loop.close();
        }
        TargetCheckpointRootVerifier.auditIndependentLedger(
                physicalDb,
                scope.shard(),
                new CheckpointManifestLimits(1_000, 256L << 20, 256L << 20, 1_024, 1 << 20, 1_000, 1_024),
                new TargetCheckpointRootVerifier.QuotaAuditLimits(100_000, 256L << 20),
                new TargetCheckpointRootVerifier.LedgerAuditLimits(100_000, 256L << 20, 500_000, 256L << 20));
        final var orphan = TargetMessageRecord.decode(vector("target-identity-vectors.properties", "message.initial"));
        assertFalse(orphan.runtime().terminal());
        assertTrue(orphan.runtime().timeline() != null);
        try (var resources = new SharedRocksDbResources(config);
                var corrupt = ShardStore.openTarget(config, scope.shard(), resources)) {
            assertNull(corrupt.get(ColumnFamily.ID, orphan.encodedKey()));
            corrupt.write(batch -> batch.put(
                    ColumnFamily.ID,
                    orphan.encodedKey(),
                    TargetValueEnvelope.encode(TargetMessageRecord.VALUE_TYPE, orphan.canonicalBytes())));
        }
        assertTrue(assertThrows(
                        IllegalStateException.class,
                        () -> TargetCheckpointRootVerifier.auditIndependentLedger(
                                physicalDb,
                                scope.shard(),
                                new CheckpointManifestLimits(1_000, 256L << 20, 256L << 20, 1_024, 1 << 20, 1_000,
                                        1_024),
                                new TargetCheckpointRootVerifier.QuotaAuditLimits(100_000, 256L << 20),
                                new TargetCheckpointRootVerifier.LedgerAuditLimits(
                                        100_000, 256L << 20, 500_000, 256L << 20)))
                .getMessage()
                .contains("Message lacks its exact current timeline index"));
        try (var resources = new SharedRocksDbResources(config);
                var restored = ShardStore.openTarget(config, scope.shard(), resources)) {
            restored.write(batch -> batch.delete(ColumnFamily.ID, orphan.encodedKey()));
        }
        final byte[] retainedBindingKey;
        final byte[] retainedBindingRaw;
        try (var resources = new SharedRocksDbResources(config);
                var corrupt = ShardStore.openTarget(config, scope.shard(), resources)) {
            final var messages = corrupt.scan(
                    ColumnFamily.ID,
                    new byte[] {TargetKeyCodec.MESSAGE_TAG, TargetKeyCodec.KEY_FORMAT},
                    new byte[] {TargetKeyCodec.SCHEDULE_BINDING_TAG, TargetKeyCodec.KEY_FORMAT},
                    1);
            assertFalse(messages.isEmpty());
            final var message = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                            messages.getFirst().value(), TargetMessageRecord.VALUE_TYPE)
                    .payload());
            retainedBindingKey = TargetKeyCodec.scheduleBinding(message.locator().scheduleBindingDigest());
            retainedBindingRaw = corrupt.get(ColumnFamily.ID, retainedBindingKey);
            assertTrue(retainedBindingRaw != null);
            corrupt.write(batch -> batch.delete(ColumnFamily.ID, retainedBindingKey));
        }
        TargetCheckpointRootVerifier.auditQuotaProjections(
                physicalDb,
                scope.shard(),
                new CheckpointManifestLimits(1_000, 256L << 20, 256L << 20, 1_024, 1 << 20, 1_000, 1_024),
                new TargetCheckpointRootVerifier.QuotaAuditLimits(100_000, 256L << 20));
        assertTrue(assertThrows(
                        IllegalStateException.class,
                        () -> TargetCheckpointRootVerifier.auditIndependentLedger(
                                physicalDb,
                                scope.shard(),
                                new CheckpointManifestLimits(1_000, 256L << 20, 256L << 20, 1_024, 1 << 20, 1_000,
                                        1_024),
                                new TargetCheckpointRootVerifier.QuotaAuditLimits(100_000, 256L << 20),
                                new TargetCheckpointRootVerifier.LedgerAuditLimits(
                                        100_000, 256L << 20, 500_000, 256L << 20)))
                .getMessage()
                .contains("Message lacks its original Schedule binding"));
        try (var resources = new SharedRocksDbResources(config);
                var restored = ShardStore.openTarget(config, scope.shard(), resources)) {
            restored.write(batch -> batch.put(ColumnFamily.ID, retainedBindingKey, retainedBindingRaw));
        }
        try (var resources = new SharedRocksDbResources(config);
                var reopened = ShardStore.openTarget(config, scope.shard(), resources)) {
            final var priorOwner = reopenOwner[0];
            final var activeReopened = priorOwner
                    .leases()
                    .transition(
                            priorOwner
                                    .leases()
                                    .acquire(
                                            priorOwner.assignment(), "close-reopen-worker", bytes(32, 0x45), 101, 10000)
                                    .orElseThrow(),
                            ShardLifecycleState.ACTIVE_FOR_COMMANDS)
                    .orElseThrow();
            assertEquals(priorOwner.priorEpoch() + 1, activeReopened.ownerEpoch());
            reopened.recordOpenedOwnerEpoch(activeReopened.ownerEpoch());
            final TargetStoreBackend.ReadAuthority ownerReads =
                    (actual, actualScope) -> new TargetStoreBackend.CommitGuard() {
                        @Override
                        public void requireCurrent() {
                            assertEquals(scope, actualScope);
                            assertArrayEquals(reopened.metadata().encode(), actual.encode());
                            final var current =
                                    priorOwner.leases().current(scope.shard()).orElseThrow();
                            assertTrue(activeReopened.sameIdentity(current));
                            assertEquals(ShardLifecycleState.ACTIVE_FOR_COMMANDS, current.state());
                            assertEquals(
                                    activeReopened.ownerEpoch(),
                                    reopened.runtimeMetadata().lastOpenedOwnerEpoch());
                        }

                        @Override
                        public void close() {}
                    };
            assertThrows(
                    IllegalStateException.class,
                    () -> TargetStoreBootstrap.reopen(
                            reopened,
                            new TargetQuotaScope(scope.shard(), bytes(32, 0xef), null),
                            new TargetStoreBackend.WriteLimits(64, 2 << 20),
                            budget(),
                            (a, b) -> guard()));
            final var recovered = TargetStoreBootstrap.reopen(
                    reopened, scope, new TargetStoreBackend.WriteLimits(64, 2 << 20), budget(), ownerReads);
            final var reopenedBackend = recovered.backend();
            final var reopenedLineage = recovered.root().recoveryLineage();
            final var reopenedCloseStore = new TargetCloseStore(reopenedBackend, scope, reopenedLineage, 16, 1);
            final var reopenedControls =
                    reopenedCloseStore.reservationControls((reader, binding) -> java.util.Optional.empty());
            final var reopenedClosures =
                    new TargetReservationClosureStore(reopenedBackend, scope, reopenedLineage, 1, reopenedControls);
            final var observed = new java.util.HashMap<
                    com.nereusstream.delay.protocol.TargetPartitionId, TargetReservationClosureStore.Progress>();
            TargetReservationClosureStore.ScanCursor afterTarget = null;
            for (int i = 0; i < reopenedCloseTargets.length; i++) {
                final var discovered = reopenedClosures.discoverNextTarget(budget(), afterTarget, ownerReads);
                assertNull(observed.put(discovered.target().orElseThrow(), discovered.progress()));
                afterTarget = discovered.nextCursor();
            }
            assertEquals(
                    java.util.Map.of(
                            reopenedCloseTargets[0], TargetReservationClosureStore.Progress.COMPLETE,
                            reopenedCloseTargets[1], TargetReservationClosureStore.Progress.COMPLETE,
                            reopenedCloseTargets[2], TargetReservationClosureStore.Progress.OPEN),
                    observed);
            assertTrue(reopenedClosures
                    .discoverNextTarget(budget(), afterTarget, ownerReads)
                    .target()
                    .isEmpty());
            final long beforeReopenedGc = reopened.latestSequenceNumber();
            final long sourceSequenceBeforeReopenedGc = reopened.shardMutationSequence();
            final var sourceBeforeReopenedGc = reopened.appliedShardLogPosition();
            final var reopenedWorkClasses = workClasses();
            final var reopenedRuntime = new TargetSourceApplyRuntime(
                    recovered,
                    reopened,
                    priorOwner.assignment(),
                    activeReopened,
                    new TargetSourceApplyRuntime.Authorities(
                            priorOwner.leases(),
                            SourceReplaySuccessor.strictKafka(),
                            entry -> {
                                throw new AssertionError("reopened GC cannot resolve a grant");
                            },
                            entry -> {
                                throw new AssertionError("reopened GC cannot resolve a fence");
                            },
                            entry -> {
                                throw new AssertionError("reopened GC cannot resolve a Close control");
                            },
                            entry -> {
                                throw new AssertionError("reopened GC cannot resolve membership issuance");
                            },
                            (a, b, c) -> guard(),
                            ownerReads,
                            entry -> {
                                throw new AssertionError("reopened GC cannot resolve a command");
                            }),
                    new TargetSourceApplyRuntime.Limits(4096, 32L << 20, 60_000_000_000L, 16, 1),
                    System::nanoTime);
            final var reopenedCursorDelta = new java.util.concurrent.atomic.AtomicReference<TargetQuotaDelta>();
            final var cutCurrent = new java.util.concurrent.atomic.AtomicBoolean(true);
            final SourceRecordConsumer simulatedDurableSource = new SourceRecordConsumer() {
                @Override
                public java.util.Optional<PolledSourceRecord> poll() {
                    return java.util.Optional.empty();
                }

                @Override
                public CheckpointCut checkpointCut(final SourcePosition expected) {
                    if (!Bytes.constantTimeEquals(
                            expected.canonicalBytes(), reopened.appliedShardLogPosition().canonicalBytes())) {
                        throw new IllegalStateException("checkpoint source differs from the Store");
                    }
                    return new CheckpointCut() {
                        @Override
                        public SourcePosition position() {
                            return expected;
                        }

                        @Override
                        public void requireCurrent() {
                            if (!cutCurrent.get()) {
                                throw new IllegalStateException("checkpoint source proof changed");
                            }
                        }
                    };
                }
            };
            final var reopenedWorker = new TargetWorkerShardRuntime(
                    simulatedDurableSource,
                    reopenedWorkClasses,
                    reopened,
                    reopened.sharedResources(),
                    reopenedRuntime,
                    new TargetWorkerShardRuntime.Maintenance(
                            reopenedControls,
                            new TargetReservationClosureWorkClassExecutor.Limits(4096, 250_000, 60_000_000_000L),
                            new TargetReservationExpiryWorkClassExecutor.Limits(2048, 100_000, 60_000_000_000L),
                            (a, b, c) -> guard(),
                            ignored -> {
                                throw new AssertionError("reopened completed Close cannot materialize");
                            },
                            ignored -> {
                                throw new AssertionError("reopened completed Close cannot expire");
                            },
                            reopenedCursorDelta::set,
                            () -> 101));
            final var reopenedFleet = new TargetWorkerShardFleetRuntime(
                    reopenedWorkClasses, reopened.sharedResources(), java.util.List.of(reopenedWorker));
            assertEquals(java.util.List.of(scope.shard()), reopenedFleet.shardIds());
            assertEquals(
                    SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE,
                    reopenedFleet
                            .runNextSourceTurn(new SchedulerBudget(1, 1, 60_000_000_000L), () -> 101)
                            .result()
                            .status());
            assertTrue(reopenedWorker.pendingSourceEntry().isEmpty());
            assertTrue(reopenedWorker
                    .settlePendingSourceTurn(new SchedulerBudget(1, 1, 60_000_000_000L), () -> 101)
                    .isEmpty());
            final var queued = reopenedFleet
                    .runNextMaintenanceTurn(new SchedulerBudget(1, 1, 60_000_000_000L))
                    .result();
            assertTrue(queued.pending());
            assertEquals(TargetReservationGcRuntime.Lane.CLOSE, queued.lane());
            assertEquals(beforeReopenedGc, reopened.latestSequenceNumber());
            final var kinds = new java.util.ArrayList<TargetReservationClosureWorkClassExecutor.Kind>();
            final var expiryKinds = new java.util.ArrayList<TargetReservationExpiryWorkClassExecutor.Kind>();
            for (int i = 0; i < 2 * (reopenedCloseTargets.length + 1); i++) {
                final var turn = reopenedFleet
                        .runNextMaintenanceTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L))
                        .result();
                assertFalse(turn.pending());
                if (i == 0) {
                    assertEquals(queued.task(), turn.task());
                }
                if (turn.lane() == TargetReservationGcRuntime.Lane.CLOSE) {
                    kinds.add(turn.closeResult().orElseThrow().kind());
                } else {
                    expiryKinds.add(turn.expiryResult().orElseThrow().kind());
                }
            }
            assertEquals(
                    2,
                    java.util.Collections.frequency(
                            kinds, TargetReservationClosureWorkClassExecutor.Kind.SKIPPED_COMPLETE));
            assertEquals(
                    1,
                    java.util.Collections.frequency(
                            kinds, TargetReservationClosureWorkClassExecutor.Kind.RESERVATIONS_COMPLETE));
            assertEquals(TargetReservationClosureWorkClassExecutor.Kind.SWEEP_COMPLETE, kinds.getLast());
            assertEquals(
                    java.util.List.of(
                            TargetReservationExpiryWorkClassExecutor.Kind.SWEEP_COMPLETE,
                            TargetReservationExpiryWorkClassExecutor.Kind.SWEEP_COMPLETE,
                            TargetReservationExpiryWorkClassExecutor.Kind.SWEEP_COMPLETE,
                            TargetReservationExpiryWorkClassExecutor.Kind.SWEEP_COMPLETE),
                    expiryKinds);
            assertEquals(0, reopenedWorkClasses.registeredActions());
            assertEquals(5, reopened.latestSequenceNumber() - beforeReopenedGc);
            assertEquals(sourceSequenceBeforeReopenedGc, reopened.shardMutationSequence());
            assertEquals(sourceBeforeReopenedGc, reopened.appliedShardLogPosition());
            assertTrue(reopenedCursorDelta.get().mutation().reservationCloseCursor());
            assertEquals(
                    TargetReservationClosureStore.Progress.COMPLETE,
                    reopenedClosures.progress(budget(), reopenedCloseTargets[2], ownerReads));
            if (!claimed && !rescheduled) {
                final var candidateId = bytes(16, 0x6c);
                final var pendingCandidate = new CheckpointUploadIntent(
                        new ShardSubject(scope.shard()),
                        reopenedLineage,
                        candidateId,
                        new OwnerIdentity(
                                bytes(8, 0x6d),
                                bytes(8, 0x6e),
                                activeReopened.ownerEpoch(),
                                activeReopened.leaseToken()),
                        reopened.metadata().storeIncarnation(),
                        bytes(32, 0x6f),
                        1,
                        null,
                        null,
                        new ProfileRef(bytes(8, 0x70), 1, bytes(32, 0x71), ProfileKind.OBJECT_STORE),
                        new TrustedUtcIntervalEvidence(
                                1,
                                2,
                                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                                bytes(32, 0x72),
                                1,
                                1,
                                1,
                                bytes(32, 0x73),
                                0,
                                null),
                        5_000,
                        CheckpointUploadState.PENDING_UPLOAD,
                        1,
                        null,
                        null);
                final var candidateIntents = new CheckpointUploadIntentStore();
                candidateIntents.create(pendingCandidate);
                final var candidatePath = root.resolve("reopened-worker-candidate");
                final var candidateLimits =
                        new CheckpointManifestLimits(1_000, 256L << 20, 256L << 20, 1_024, 1 << 20, 1_000, 1_024);
                final var quotaLimits = new TargetCheckpointRootVerifier.QuotaAuditLimits(100_000, 256L << 20);
                final var ledgerLimits = new TargetCheckpointRootVerifier.LedgerAuditLimits(
                        100_000, 256L << 20, 500_000, 256L << 20);
                final long beforeCandidate = reopened.operationStatistics().nativeWriteCalls();
                cutCurrent.set(false);
                assertThrows(
                        IllegalStateException.class,
                        () -> reopenedWorker.submitProtectedCheckpointCandidate(
                                candidateIntents, () -> 101, candidatePath, pendingCandidate, candidateLimits,
                                quotaLimits, ledgerLimits));
                cutCurrent.set(true);
                final var candidate = reopenedWorker.submitProtectedCheckpointCandidate(
                        candidateIntents,
                        () -> 101,
                        candidatePath,
                        pendingCandidate,
                        candidateLimits,
                        quotaLimits,
                        ledgerLimits);
                assertEquals(beforeCandidate, reopened.operationStatistics().nativeWriteCalls());
                assertThrows(
                        IllegalStateException.class,
                        () -> reopenedWorker.runSourceTurn(new SchedulerBudget(1, 1, 1_000), () -> 101));
                assertThrows(
                        IllegalStateException.class,
                        () -> reopenedWorker.runMaintenanceTurn(new SchedulerBudget(1, 1, 1_000)));
                assertThrows(IllegalStateException.class, reopenedWorker::pauseNewTurns);
                assertTrue(reopenedWorker.runCheckpointTurn(new SchedulerBudget(1, 1, 1_000)).isEmpty());
                assertEquals(beforeCandidate, reopened.operationStatistics().nativeWriteCalls());
                assertEquals(
                        candidatePath,
                        reopenedWorker
                                .runCheckpointTurn(new SchedulerBudget(1, candidate.task().bytes(), 1_000))
                                .orElseThrow()
                                .checkpointPath());
                assertEquals(candidatePath, candidate.outcome().orElseThrow().checkpointPath());
                assertEquals(beforeCandidate + 1, reopened.operationStatistics().nativeWriteCalls());
                final var staleCut = reopenedWorker.submitProtectedCheckpointCandidate(
                        candidateIntents,
                        () -> 101,
                        candidatePath,
                        pendingCandidate,
                        candidateLimits,
                        quotaLimits,
                        ledgerLimits);
                cutCurrent.set(false);
                assertTrue(reopenedWorker
                        .runCheckpointTurn(new SchedulerBudget(1, staleCut.task().bytes(), 1_000))
                        .orElseThrow()
                        .failure() instanceof IllegalStateException);
                assertEquals(beforeCandidate + 1, reopened.operationStatistics().nativeWriteCalls());
                cutCurrent.set(true);
                final var reused = reopenedWorker.submitProtectedCheckpointCandidate(
                        candidateIntents,
                        () -> 101,
                        candidatePath,
                        pendingCandidate,
                        candidateLimits,
                        quotaLimits,
                        ledgerLimits);
                assertEquals(
                        candidatePath,
                        reopenedWorker
                                .runCheckpointTurn(new SchedulerBudget(1, reused.task().bytes(), 1_000))
                                .orElseThrow()
                                .checkpointPath());
                assertEquals(candidatePath, reused.outcome().orElseThrow().checkpointPath());
                assertEquals(beforeCandidate + 1, reopened.operationStatistics().nativeWriteCalls());
            }
            final long beforePausedGc = reopened.latestSequenceNumber();
            final var pausedSubmission = reopenedFleet
                    .runNextMaintenanceTurn(new SchedulerBudget(1, 1, 60_000_000_000L))
                    .result();
            assertTrue(pausedSubmission.pending());
            reopenedWorker.pauseNewTurns();
            assertThrows(
                    IllegalStateException.class,
                    () -> reopenedFleet.runNextMaintenanceTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L)));
            assertThrows(
                    IllegalStateException.class,
                    () -> reopenedFleet.runNextSourceTurn(new SchedulerBudget(1, 1, 60_000_000_000L), () -> 101));
            final var pendingDrain = reopenedWorker.drain(
                    new TargetOwnerDrainCoordinator.Request(5_000, new SchedulerBudget(1, 1, 60_000_000_000L)),
                    () -> 101);
            assertEquals(TargetOwnerDrainCoordinator.Status.PENDING_GC, pendingDrain.status());
            assertEquals(pausedSubmission.task(), pendingDrain.pendingGcTask());
            assertFalse(reopened.isClosed());
            assertEquals(beforePausedGc, reopened.latestSequenceNumber());
            assertEquals(
                    ShardLifecycleState.ACTIVE_FOR_COMMANDS,
                    priorOwner.leases().current(scope.shard()).orElseThrow().state());
            final boolean uncertainStore = !claimed && rescheduled;
            if (uncertainStore) {
                assertThrows(
                        ShardStore.RocksDbWriteFailure.class,
                        () -> reopened.write(batch -> batch.put(
                                ColumnFamily.META,
                                com.nereusstream.delay.store.KeyCodec.metaFixed(4),
                                Bytes.utf8("malformed-target-drain-fence"))));
                assertTrue(reopened.isWriteOutcomeUncertain());
            }
            final com.nereusstream.delay.ownership.OwnerLease replacementOwner;
            if (claimed && rescheduled) {
                assertTrue(priorOwner.leases().release(activeReopened));
                replacementOwner = priorOwner
                        .leases()
                        .transition(
                                priorOwner
                                        .leases()
                                        .acquire(
                                                priorOwner.assignment(),
                                                "replacement-close-worker",
                                                bytes(32, 0x47),
                                                102,
                                                10_000)
                                        .orElseThrow(),
                                ShardLifecycleState.ACTIVE_FOR_COMMANDS)
                        .orElseThrow();
            } else {
                replacementOwner = null;
            }
            if (!claimed && !rescheduled) {
                priorOwner.loseTransitionResponse().set(true);
                priorOwner.loseReleaseResponse().set(true);
            }
            if (claimed && !rescheduled) {
                priorOwner.throwTransitionAfterCommit().set(true);
                priorOwner.throwReleaseAfterCommit().set(true);
                assertThrows(
                        IllegalStateException.class,
                        () -> reopenedWorker.drain(
                                new TargetOwnerDrainCoordinator.Request(
                                        5_000, new SchedulerBudget(100, 2_000_000, 60_000_000_000L)),
                                () -> 101));
                assertTrue(reopenedRuntime.fenced());
                assertFalse(reopened.isClosed());
                assertEquals(
                        ShardLifecycleState.DRAINING,
                        priorOwner.leases().current(scope.shard()).orElseThrow().state());
                assertThrows(
                        IllegalStateException.class,
                        () -> reopenedWorker.drain(
                                new TargetOwnerDrainCoordinator.Request(
                                        5_000, new SchedulerBudget(100, 2_000_000, 60_000_000_000L)),
                                () -> 101));
                assertTrue(reopened.isClosed());
                assertTrue(priorOwner.leases().current(scope.shard()).isEmpty());
            }
            final var completedDrain = reopenedWorker.drain(
                    new TargetOwnerDrainCoordinator.Request(
                            5_000, new SchedulerBudget(100, 2_000_000, 60_000_000_000L)),
                    () -> 101);
            assertEquals(
                    replacementOwner == null
                            ? uncertainStore
                                    ? TargetOwnerDrainCoordinator.Status.UNCERTAIN_RELEASED
                                    : TargetOwnerDrainCoordinator.Status.RELEASED
                            : TargetOwnerDrainCoordinator.Status.OWNER_LOST_CLOSED,
                    completedDrain.status());
            assertFalse(priorOwner.loseTransitionResponse().get());
            assertFalse(priorOwner.loseReleaseResponse().get());
            assertFalse(priorOwner.throwTransitionAfterCommit().get());
            assertFalse(priorOwner.throwReleaseAfterCommit().get());
            assertNull(completedDrain.pendingGcTask());
            assertEquals(0, reopenedWorkClasses.registeredActions());
            assertTrue(reopened.isClosed());
            if (replacementOwner == null) {
                assertTrue(priorOwner.leases().current(scope.shard()).isEmpty());
            } else {
                assertTrue(replacementOwner.sameIdentity(
                        priorOwner.leases().current(scope.shard()).orElseThrow()));
            }
            assertEquals(
                    completedDrain.status(),
                    reopenedWorker
                            .drain(
                                    new TargetOwnerDrainCoordinator.Request(
                                            5_000, new SchedulerBudget(1, 1, 60_000_000_000L)),
                                    () -> 101)
                            .status());
            if (uncertainStore) {
                assertThrows(
                        IllegalArgumentException.class, () -> ShardStore.openTarget(config, scope.shard(), resources));
            } else {
                try (var afterDrain = ShardStore.openTarget(config, scope.shard(), resources)) {
                    assertEquals(sourceSequenceBeforeReopenedGc, afterDrain.shardMutationSequence());
                    assertEquals(sourceBeforeReopenedGc, afterDrain.appliedShardLogPosition());
                }
            }
        }
    }

    private static Signed signedClose(
            TargetCloseRequest request,
            byte[] operation,
            ControlAuthorizationContext actor,
            KeyPair keys,
            long retryUntil) {
        final var ref = new ControlRef(
                operation,
                PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()),
                0);
        final var body = new TargetCloseBody(request.shards().getFirst().shard(), retryUntil, ref, request);
        final var mutation = SystemMutation.signed(
                body.shard(),
                SystemMutationType.APPLY_SHARD_CONTROL,
                body.retryUntil(),
                body.logicalIdentity(),
                body.canonicalBytes(),
                AuthorIdentity.control(actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash())
                        .canonicalBytes(),
                1,
                keys.getPrivate());
        final var target = new ControlTargetRef(
                0,
                ControlTargetKind.SHARD,
                new ShardSubject(body.shard()),
                mutation.systemMutationId(),
                mutation.mutationHash());
        return new Signed(
                PreparedControlOperation.prepare(
                        operation,
                        request.operationKind(),
                        new ControlAuthor(
                                actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash()),
                        request.operationRequest(),
                        List.of(target),
                        1,
                        400,
                        1,
                        keys.getPrivate()),
                mutation);
    }

    private static void applyClose(
            WorkerSourceApplyLoop loop,
            java.util.ArrayDeque<SourceRecordConsumer.PolledSourceRecord> entries,
            Signed close,
            KafkaSourcePosition at) {
        entries.add(new SourceRecordConsumer.PolledSourceRecord(
                new SourceReplayMutation(close.mutation(), at, null, null),
                (entry, outcome) -> SourceAcknowledgement.AcknowledgementResult.acked()));
        final var turn = loop.runTurn(new SchedulerBudget(1, 1_000_000, 1_000), () -> 100);
        if (turn.failure() != null) {
            throw new AssertionError("Close apply failed: " + turn.status(), turn.failure());
        }
        assertEquals(SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED, turn.status());
        assertEquals(StableCode.OK, turn.appliedOutcome().systemMutationResult().stableCode());
    }

    private static void applyFence(
            WorkerSourceApplyLoop loop,
            java.util.ArrayDeque<SourceRecordConsumer.PolledSourceRecord> queue,
            long closeThrough,
            KafkaSourcePosition position,
            KeyPair keys) {
        final var proof = new TrustedUtcIntervalEvidence(
                closeThrough + 10,
                closeThrough + 15,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                bytes(32, 0x91),
                1,
                1,
                1,
                bytes(32, 0x92),
                0,
                new byte[0]);
        final var body = new TargetTimeFenceBody(
                position.shardId(), position.brokerLogAppendTimeEpochMs() + 1000, closeThrough, 1, proof);
        final var mutation = SystemMutation.signed(
                position.shardId(),
                SystemMutationType.TIME_FENCE,
                body.retryUntil(),
                body.proofId(),
                body.canonicalBytes(),
                AuthorIdentity.fence(bytes(32, 0x93), 1).canonicalBytes(),
                1,
                keys.getPrivate());
        queue.add(new SourceRecordConsumer.PolledSourceRecord(
                new SourceReplayMutation(mutation, position, null, null),
                (entry, outcome) -> SourceAcknowledgement.AcknowledgementResult.acked()));
        final var turn = loop.runTurn(new SchedulerBudget(1, 1_000_000, 1_000), () -> 100);
        if (turn.failure() != null) {
            throw new AssertionError("fence apply failed: " + turn.status(), turn.failure());
        }
        assertEquals(SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED, turn.status());
        assertEquals(StableCode.OK, turn.appliedOutcome().systemMutationResult().stableCode());
    }

    private static TargetCommandStore.PayloadProofControls noProofs() {
        return (binding, source) -> {
            throw new AssertionError("unexpected proof authority resolution");
        };
    }

    private static PreparedCommand commit(CanonicalPayloadCommitProof proof, KafkaSourcePosition at, int unique) {
        final var id = cancel(proof.delayMessageId(), at, unique).commandId();
        final var body = new CommitLargeScheduleBody(
                proof.delayMessageId(), at.brokerLogAppendTimeEpochMs() + 1000, proof.reservationId(), proof);
        final var tuple = ProtocolTuple.managedCommand();
        return new PreparedCommand(
                at.shardId(),
                id,
                proof.delayMessageId(),
                CommandType.COMMIT_LARGE_SCHEDULE,
                tuple,
                body.retryUntilEpochMs(),
                body.canonicalBytes(),
                CommandHash.compute(
                        tuple,
                        CommandType.COMMIT_LARGE_SCHEDULE,
                        id,
                        proof.delayMessageId(),
                        body.retryUntilEpochMs(),
                        body.canonicalBytes()));
    }

    private static PreparedCommand cancel(DelayMessageId message, KafkaSourcePosition at, int unique) {
        final var id = new CommandId(SelfRoutingId.fromLogicalUuid(
                        at.shardId(),
                        new java.util.UUID(
                                (at.brokerLogAppendTimeEpochMs() << 16) | 0x7000L | unique,
                                0x8000000000000000L | unique))
                .bytes());
        final var body = new CancelCommandBody(
                message, at.brokerLogAppendTimeEpochMs() + 1000, new MessagePrecondition(null, null));
        final var tuple = ProtocolTuple.managedCommand();
        return new PreparedCommand(
                at.shardId(),
                id,
                message,
                CommandType.CANCEL,
                tuple,
                body.retryUntilEpochMs(),
                body.canonicalBytes(),
                CommandHash.compute(
                        tuple, CommandType.CANCEL, id, message, body.retryUntilEpochMs(), body.canonicalBytes()));
    }

    private static PreparedCommand schedule(
            CanonicalScheduleIntent intent, DelayMessageId seed, KafkaSourcePosition at, int unique) {
        final var id = cancel(seed, at, unique).commandId();
        final var message =
                new DelayMessageId(cancel(seed, at, unique + 1000).commandId().bytes());
        final var body = new ScheduleCommandBody(message, at.brokerLogAppendTimeEpochMs() + 1000, intent);
        final var tuple = ProtocolTuple.managedCommand();
        return new PreparedCommand(
                at.shardId(),
                id,
                message,
                CommandType.SCHEDULE,
                tuple,
                body.retryUntilEpochMs(),
                body.canonicalBytes(),
                CommandHash.compute(
                        tuple, CommandType.SCHEDULE, id, message, body.retryUntilEpochMs(), body.canonicalBytes()));
    }

    private static PreparedCommand reschedule(DelayMessageId message, KafkaSourcePosition at, int unique) {
        return reschedule(message, at, unique, new MessagePrecondition(null, null));
    }

    private static PreparedCommand reschedule(
            DelayMessageId message, KafkaSourcePosition at, int unique, MessagePrecondition precondition) {
        final var id = cancel(message, at, unique).commandId();
        final var body = new RescheduleCommandBody(
                message,
                at.brokerLogAppendTimeEpochMs() + 1000,
                precondition,
                at.brokerLogAppendTimeEpochMs() + 100,
                at.brokerLogAppendTimeEpochMs() + 2000);
        final var tuple = ProtocolTuple.managedCommand();
        return new PreparedCommand(
                at.shardId(),
                id,
                message,
                CommandType.RESCHEDULE,
                tuple,
                body.retryUntilEpochMs(),
                body.canonicalBytes(),
                CommandHash.compute(
                        tuple, CommandType.RESCHEDULE, id, message, body.retryUntilEpochMs(), body.canonicalBytes()));
    }

    private static byte[] vector(String name, String key) throws Exception {
        final var values = new Properties();
        try (var stream = TargetCommandStoreTest.class.getResourceAsStream("/ndip3/" + name)) {
            values.load(stream);
        }
        return HexFormat.of().parseHex(values.getProperty(key));
    }

    private static SourceApplyCoordinator.TurnResult apply(
            WorkerSourceApplyLoop loop,
            java.util.ArrayDeque<SourceRecordConsumer.PolledSourceRecord> queue,
            PreparedCommand command,
            KafkaSourcePosition position) {
        queue.add(new SourceRecordConsumer.PolledSourceRecord(
                new SourceReplayRecord(command, position, null, null), (entry, outcome) -> {
                    assertArrayEquals(
                            position.canonicalBytes(), outcome.commandResult().appliedSourcePosition());
                    return SourceAcknowledgement.AcknowledgementResult.acked();
                }));
        final var result = loop.runTurn(new SchedulerBudget(1, 1_000_000, 1_000), () -> 100);
        if (result.failure() != null) {
            throw new AssertionError("source apply failed: " + result.status(), result.failure());
        }
        return result;
    }

    private static WorkClassExecutionRegistry workClasses() {
        final var policies = new java.util.EnumMap<WorkClass, WorkClassPolicy>(WorkClass.class);
        for (var workClass : WorkClass.values()) {
            final boolean protectedClass = workClass != WorkClass.QUERY && workClass != WorkClass.CHECKPOINT;
            policies.put(
                    workClass,
                    new WorkClassPolicy(
                            1,
                            1,
                            1_000_000,
                            1,
                            1_000_000,
                            1_000,
                            protectedClass ? 1 : 0,
                            protectedClass ? 1 : 0,
                            workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies, 100, 100, 16, 2_000_000), () -> 0);
    }

    private static TargetQuotaGrantControlVerifier.Authority authority(
            InMemoryControlTargetRegistrationAuthority registrations,
            KeyPair keys,
            ControlAuthorizationContext actor,
            KafkaSourcePosition source,
            TargetQuotaGrantControlRequest request,
            TargetQuotaGrantControlVerifier.CapacityAuthority capacity) {
        return new TargetQuotaGrantControlVerifier.Authority(
                registrations,
                (version, position) -> version == 1 ? keys.getPublic() : null,
                (scope, position) -> {
                    scope.requireRoute(source.shardId(), request.next().scope().tenantScope());
                    if (!source.sameSourceIdentity(position)) {
                        throw new IllegalStateException("foreign source");
                    }
                },
                capacity,
                actor,
                prepared -> true);
    }

    private record Signed(PreparedControlOperation control, SystemMutation mutation) {}

    private static Signed signed(
            TargetQuotaGrantControlRequest request, byte[] operation, ControlAuthorizationContext actor, KeyPair keys) {
        return signed(request, operation, actor, keys, 500);
    }

    private static Signed signed(
            TargetQuotaGrantControlRequest request,
            byte[] operation,
            ControlAuthorizationContext actor,
            KeyPair keys,
            long retryUntil) {
        final var ref = new ControlRef(
                operation,
                PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()),
                0);
        final var body = new TargetQuotaGrantControlBody(request.next().scope().shard(), retryUntil, ref, request);
        final var mutation = SystemMutation.signed(
                body.shard(),
                SystemMutationType.APPLY_SHARD_CONTROL,
                body.retryUntil(),
                body.logicalIdentity(),
                body.canonicalBytes(),
                AuthorIdentity.control(actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash())
                        .canonicalBytes(),
                1,
                keys.getPrivate());
        final var target = new ControlTargetRef(
                0,
                ControlTargetKind.SHARD,
                new ShardSubject(body.shard()),
                mutation.systemMutationId(),
                mutation.mutationHash());
        final var control = PreparedControlOperation.prepare(
                operation,
                request.operationKind(),
                new ControlAuthor(actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash()),
                request.operationRequest(),
                List.of(target),
                1,
                400,
                1,
                keys.getPrivate());
        return new Signed(control, mutation);
    }

    private static byte[] systemKey(SystemMutation mutation) {
        return Bytes.concat(new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, 1}, mutation.systemMutationId());
    }

    private static KafkaSourcePosition source(KafkaSourcePosition from, long offset, long time) {
        return new KafkaSourcePosition(
                from.shardId(),
                from.authenticatedClusterId(),
                from.nativeTopicUuid(),
                offset,
                from.leaderEpoch(),
                time);
    }

    private static BoundedReadBudget budget() {
        return new BoundedReadBudget(2048, 32L << 20, 60_000_000_000L, System::nanoTime);
    }

    private static TargetStoreBackend.CommitGuard guard() {
        return new TargetStoreBackend.CommitGuard() {
            @Override
            public void requireCurrent() {}

            @Override
            public void close() {}
        };
    }

    private static byte[] bytes(int size, int value) {
        final var data = new byte[size];
        Arrays.fill(data, (byte) value);
        return data;
    }

    private static byte[] raw(String key) throws Exception {
        final var values = new Properties();
        try (var stream =
                TargetCommandStoreTest.class.getResourceAsStream("/ndip3/target-quota-grant-vectors.properties")) {
            values.load(stream);
        }
        return HexFormat.of().parseHex(values.getProperty(key));
    }
}
