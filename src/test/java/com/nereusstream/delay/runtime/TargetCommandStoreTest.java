package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import com.nereusstream.delay.ownership.TargetMessageExpiryWorkClassExecutor;
import com.nereusstream.delay.ownership.TargetOwnerDrainCoordinator;
import com.nereusstream.delay.ownership.TargetReservationClosureWorkClassExecutor;
import com.nereusstream.delay.ownership.TargetReservationExpiryWorkClassExecutor;
import com.nereusstream.delay.ownership.TargetReservationGcRuntime;
import com.nereusstream.delay.ownership.TargetReservationQueryWorkClassExecutor;
import com.nereusstream.delay.ownership.TargetSourceApplyRuntime;
import com.nereusstream.delay.ownership.TargetWorkerHostTestBridge;
import com.nereusstream.delay.ownership.TargetWorkerOrdinaryDrr;
import com.nereusstream.delay.ownership.TargetWorkerOwnerActivation;
import com.nereusstream.delay.ownership.TargetWorkerShardFactory;
import com.nereusstream.delay.ownership.TargetWorkerShardFleetRuntime;
import com.nereusstream.delay.ownership.TargetWorkerShardRuntime;
import com.nereusstream.delay.ownership.TargetWorkerTargetInventory;
import com.nereusstream.delay.ownership.WorkerSourceApplyLoop;
import com.nereusstream.delay.protocol.AcknowledgementSet;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CancelCommandBody;
import com.nereusstream.delay.protocol.CanonicalPayloadCommitProof;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
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
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.OrderingMode;
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
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.PublishOutcomeBody;
import com.nereusstream.delay.protocol.RescheduleCommandBody;
import com.nereusstream.delay.protocol.RetryJitter;
import com.nereusstream.delay.protocol.ScheduleCommandBody;
import com.nereusstream.delay.protocol.SelfRoutingId;
import com.nereusstream.delay.protocol.ShardId;
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
import com.nereusstream.delay.protocol.TargetDomainState;
import com.nereusstream.delay.protocol.TargetExpireGenerationBody;
import com.nereusstream.delay.protocol.TargetMembershipGrant;
import com.nereusstream.delay.protocol.TargetMembershipPolicy;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetNativePolicyScope;
import com.nereusstream.delay.protocol.TargetPartitionHashInput;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetPartitionPolicy;
import com.nereusstream.delay.protocol.TargetPublishAdmissionBody;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.protocol.TargetQuotaGrant;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlBody;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
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
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetStoreBackendFailureTestBridge;
import com.nereusstream.delay.store.TargetValueEnvelope;
import com.nereusstream.delay.store.WorkerRuntimeTestSupport;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.HashSet;
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
    private OwnerLeaseStore ownerDelegate;
    private byte[] connectedOwnerSession;
    private OxiaOwnerLeaseStore takeoverLeases;
    private byte[] takeoverSession;

    @org.junit.jupiter.api.Test
    @org.junit.jupiter.api.Tag("real-service")
    void realOxiaPublishOwnerTakeoverRetainsUncertainBudgetAndBarrier() throws Exception {
        runRealOxiaTargetOutcome(10);
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(ints = {12, 15, 17})
    @org.junit.jupiter.api.Tag("real-service")
    void realOxiaLateTargetOutcomeRetainsTerminalDecision(int mode) throws Exception {
        runRealOxiaTargetOutcome(mode);
    }

    private void runRealOxiaTargetOutcome(int mode) throws Exception {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        org.junit.jupiter.api.Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(),
                "NEREUS_DELAY_OXIA_ENDPOINT is not configured");
        final String configuredNamespace = System.getenv("NEREUS_DELAY_OXIA_NAMESPACE");
        final String namespace = configuredNamespace == null || configuredNamespace.isBlank()
                ? "default" : configuredNamespace;
        final String prefix = "nereus-delay-target-publish-recovery/" + java.util.UUID.randomUUID();
        try (var first = com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend.connect(
                endpoint, namespace, "target-publish-owner-a", java.time.Duration.ofSeconds(15), prefix);
                var second = com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend.connect(
                        endpoint, namespace, "target-publish-owner-b", java.time.Duration.ofSeconds(15), prefix)) {
            ownerDelegate = new OxiaOwnerLeaseStore(first.backend());
            connectedOwnerSession = first.sessionIdentity();
            takeoverLeases = new OxiaOwnerLeaseStore(second.backend());
            takeoverSession = second.sessionIdentity();
            assertFalse(Arrays.equals(connectedOwnerSession, takeoverSession));
            modifiesActualTimelineOrClaimWithHistoryAndFirstResults(
                    true, false, false, false, false, true, false, mode);
        }
    }

    private byte[] ownerSession(final int fixture) {
        return connectedOwnerSession == null ? bytes(32, fixture) : Bytes.copy(connectedOwnerSession);
    }

    @ParameterizedTest
    @CsvSource({
        "false,false,false,false,false,false,false,0", "true,false,false,false,false,false,false,0",
        "false,true,false,false,false,false,false,0", "true,true,false,false,false,false,false,0",
        "true,false,true,false,false,false,false,0",
        "true,false,false,true,false,false,false,0", "true,false,false,true,true,false,false,0",
        "true,false,false,false,false,true,false,0",
        "true,false,false,false,false,true,true,0",
        "true,false,false,false,false,true,false,1", "true,false,false,false,false,true,false,2",
        "true,false,false,false,false,true,false,3", "true,false,false,false,false,true,false,4",
        "true,false,false,false,false,true,false,5",
        "true,false,false,false,false,true,false,6", "true,false,false,false,false,true,false,7",
        "true,false,false,false,false,true,false,8", "true,false,false,false,false,true,false,9",
        "true,false,false,false,false,true,false,10", "true,false,false,false,false,true,false,11",
        "true,false,false,false,false,true,false,12", "true,false,false,false,false,true,false,13",
        "true,false,false,false,false,true,false,14", "true,false,false,false,false,true,false,15",
        "true,false,false,false,false,true,false,16", "true,false,false,false,false,true,false,17"
    })
    void modifiesActualTimelineOrClaimWithHistoryAndFirstResults(
            boolean claimed,
            boolean rescheduled,
            boolean strictOrderExpiry,
            boolean uncertainRetry,
            boolean closedRetryQueue,
            boolean publishedOutcome,
            boolean mismatchedTransfer,
            int publishFailure)
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
        final SourceReplayMutation[] uncertainEntryForRecovery = new SourceReplayMutation[1];
        final Path physicalDb;
        try (var resources = WorkerRuntimeTestSupport.openWithSyntheticObservation(config);
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
            // This Shard fixture needs room for the strict-domain follower used after Admission.
            final long[] otherTargetAmounts = targetAmounts.clone();
            otherTargetAmounts[CapacityDimension.ACTIVE_MESSAGES.wireValue() - 1] = 2;
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
                            List.of(5, 6),
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
            final var membershipPolicy = new TargetMembershipPolicy(
                    scope.tenantScope(), destination.ref(), dispatch.digest(), dispatch, controls, bytes(32, 0x73));
            final var membership = new TargetMembershipGrant(
                    scope.tenantScope(),
                    destination.ref(),
                    dispatch,
                    dispatch,
                    controls,
                    membershipPolicy.digest(),
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
            // The Native scope is seeded without source-applied approval, so first binding must fall back to ordinary.
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
                                                    membershipPolicy.encodedKey(),
                                                    TargetMembershipPolicy.VALUE_TYPE,
                                                    membershipPolicy.canonicalBytes()),
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
                    strictOrderExpiry ? OrderingMode.DELIVERY_TIME_FIFO : priorIntent.orderingMode(),
                    strictOrderExpiry ? Bytes.utf8("target-expiry-order-key") : priorIntent.orderingKey(),
                    model.inlinePayload(),
                    null,
                    priorIntent.adapterMetadata(),
                    priorIntent.businessKey(),
                    priorIntent.eventTimeEpochMs(),
                    strictOrderExpiry ? NativeDeliveryPolicy.FORBID : model.nativeDeliveryPolicy());
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
                    strictOrderExpiry ? null : nativeScope.digest(),
                    strictOrderExpiry ? bytes(32, 0x67) : null);
            final var expectedBinding = new TargetScheduleBinding(
                    binding.messageId(),
                    binding.commandType(),
                    binding.canonicalBody(),
                    binding.bindingSource(),
                    binding.target(),
                    binding.domain(),
                    binding.accountingIncarnation(),
                    binding.requiredDispatchRef(),
                    binding.offeredDispatchRef(),
                    binding.controlScopeRef(),
                    binding.membershipGrantRef(),
                    null,
                    binding.orderingDomain());
            final var profiles = ProfileBindingControlState.empty()
                    .activate(destination.ref(), origin)
                    .activate(capability.ref(), grantAt);
            final var scheduleAuthority = new TargetScheduleRegistration.Authority(
                    binding,
                    physical,
                    destination,
                    capability,
                    profiles,
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
                        strictOrderExpiry ? null : nativeScope.digest(),
                        strictOrderExpiry ? bytes(32, 0x67) : null);
                return new TargetCommandStore.ScheduleAdmission(
                        StableCode.OK,
                        new TargetScheduleRegistration.Authority(
                                acceptedBinding,
                                physical,
                                destination,
                                capability,
                                profiles,
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
            if (strictOrderExpiry) {
                final byte[] orderKey = TargetKeyCodec.orderState(
                        message.locator().target(), message.locator().orderingDomain());
                final var originalOrder = TargetOrderState.decode(TargetValueEnvelope.decode(
                                store.get(ColumnFamily.META, orderKey), TargetOrderState.VALUE_TYPE)
                        .payload());
                final var admittedWork = message.runtime().timeline();
                final var watermarkedOrder = new TargetOrderState(
                        originalOrder.target(),
                        originalOrder.orderingDomain(),
                        originalOrder.sourceShard(),
                        originalOrder.executionDomain(),
                        originalOrder.accountingIncarnation(),
                        originalOrder.orderingContract(),
                        TargetQueueState.nextRevision(originalOrder.stateRevision()),
                        originalOrder.controlVersion(),
                        originalOrder.gate(),
                        admittedWork.ordinaryKey(),
                        originalOrder.serviceableHead(),
                        originalOrder.barrier());
                watermarkedOrder.requireSuccessorOf(originalOrder);
                store.write(batch -> batch.put(
                        ColumnFamily.META,
                        orderKey,
                        TargetValueEnvelope.encode(TargetOrderState.VALUE_TYPE, watermarkedOrder.canonicalBytes())));

                final var lateSource = source(
                        scheduleAt,
                        scheduleAt.offset() + 1,
                        scheduleAt.brokerLogAppendTimeEpochMs() + 1);
                final long lateDeliverAt = message.deliverAtEpochMs() - 1;
                final var lateIntent = CanonicalScheduleIntent.create(
                        destination.ref(),
                        intent.retryPolicy(),
                        lateDeliverAt,
                        lateSource.brokerLogAppendTimeEpochMs() + 2000,
                        intent.deliveryMode(),
                        intent.orderingMode(),
                        intent.orderingKey(),
                        model.inlinePayload(),
                        null,
                        intent.adapterMetadata(),
                        intent.businessKey(),
                        intent.eventTimeEpochMs(),
                        NativeDeliveryPolicy.FORBID);
                final var lateMessageId = new DelayMessageId(cancel(messageId, lateSource, 11).commandId().bytes());
                final var lateBody = new ScheduleCommandBody(
                        lateMessageId, lateSource.brokerLogAppendTimeEpochMs() + 1000, lateIntent);
                final var lateCommandId = cancel(lateMessageId, lateSource, 12).commandId();
                final var lateCommand = new PreparedCommand(
                        scope.shard(),
                        lateCommandId,
                        lateMessageId,
                        CommandType.SCHEDULE,
                        ProtocolTuple.managedCommand(),
                        lateBody.retryUntilEpochMs(),
                        lateBody.canonicalBytes(),
                        CommandHash.compute(
                                ProtocolTuple.managedCommand(),
                                CommandType.SCHEDULE,
                                lateCommandId,
                                lateMessageId,
                                lateBody.retryUntilEpochMs(),
                                lateBody.canonicalBytes()));
                final long sourceSequenceBeforeLateSchedule = store.shardMutationSequence();
                final var lateResult = initialCommands.commit(
                        initialCommands.prepareFirst(
                                budget(),
                                lateCommand,
                                lateSource,
                                initialPolicy,
                                (reader, bound, source) -> false,
                                (reader, bound) -> java.util.Optional.empty(),
                                scheduleProvider,
                                noProofs()),
                        (a, b, c) -> guard());
                assertEquals(StableCode.ORDER_BEFORE_ADMISSION_WATERMARK, lateResult.stableCode());
                assertEquals(sourceSequenceBeforeLateSchedule + 1, store.shardMutationSequence());
                assertNull(store.get(ColumnFamily.ID, TargetKeyCodec.message(lateMessageId)));
                final var actualOrder = TargetOrderState.decode(TargetValueEnvelope.decode(
                                store.get(ColumnFamily.META, orderKey), TargetOrderState.VALUE_TYPE)
                        .payload());
                assertArrayEquals(watermarkedOrder.canonicalBytes(), actualOrder.canonicalBytes());
                final long afterLateSchedule = store.latestSequenceNumber();
                final var replay = new TargetCommandReplayStore(backend, scope, lineage, 16, 1);
                final var replayedLateResult = replay.commit(
                        replay.prepareReplayOrExpired(budget(), lateCommand, lateSource).orElseThrow(),
                        (a, b, c) -> guard(),
                        (a, b) -> guard());
                assertEquals(StableCode.ORDER_BEFORE_ADMISSION_WATERMARK, replayedLateResult.stableCode());
                assertEquals(afterLateSchedule, store.latestSequenceNumber());
            }
            final long claimExecutionBytes = originalGrant
                    .accounting()
                    .accountedPublishBytes(
                            intent.adapterMetadata().kind()
                                            == com.nereusstream.delay.protocol.AdapterMetadata.Kind.KAFKA
                                    ? AdapterKind.KAFKA
                                    : AdapterKind.PULSAR,
                            message.payloadLength(),
                            binding.intent().adapterMetadata().canonicalBytes().length);
            final var locator = message.locator();
            final var work = message.runtime().timeline();
            assertFalse(work.nativeCandidate());
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
                    source(scheduleAt, scheduleAt.offset() + 2, scheduleAt.brokerLogAppendTimeEpochMs() + 2);
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
            final OwnerLeaseStore delegateLeases =
                    ownerDelegate == null ? new InMemoryOwnerLeaseStore() : ownerDelegate;
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
            final var acquiring = leases.acquire(assignment, "cancel-worker", ownerSession(0x44), 1, 10000)
                    .orElseThrow();
            final long ownerEpochBeforeActivation = store.runtimeMetadata().lastOpenedOwnerEpoch();
            final var activationWrongAssignment = new SourceAssignment(
                    assignment.shardId(),
                    bytes(32, 0x45),
                    assignment.assignmentEpoch(),
                    assignment.activationBarrier());
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetWorkerOwnerActivation.activate(
                            initialized, store, activationWrongAssignment, acquiring, leases, () -> 1));
            assertEquals(ownerEpochBeforeActivation, store.runtimeMetadata().lastOpenedOwnerEpoch());
            assertEquals(
                    ShardLifecycleState.ACQUIRING,
                    leases.current(scope.shard()).orElseThrow().state());
            if (claimed && !rescheduled && strictOrderExpiry) {
                final var clockReads = new java.util.concurrent.atomic.AtomicInteger();
                assertThrows(
                        IllegalStateException.class,
                        () -> TargetWorkerOwnerActivation.activate(
                                initialized,
                                store,
                                assignment,
                                acquiring,
                                leases,
                                () -> clockReads.getAndIncrement() == 0 ? 2 : 1));
                assertEquals(
                        ShardLifecycleState.ACQUIRING,
                        leases.current(scope.shard()).orElseThrow().state());
                assertEquals(acquiring.ownerEpoch(), store.runtimeMetadata().lastOpenedOwnerEpoch());
            }
            if (!claimed && !rescheduled && !strictOrderExpiry) {
                throwTransitionAfterCommit.set(true);
                assertThrows(
                        IllegalStateException.class,
                        () -> TargetWorkerOwnerActivation.activate(
                                initialized, store, assignment, acquiring, leases, () -> 1));
                assertEquals(
                        ShardLifecycleState.ACTIVE_FOR_COMMANDS,
                        leases.current(scope.shard()).orElseThrow().state());
            } else {
                loseTransitionResponse.set(true);
            }
            final var active = TargetWorkerOwnerActivation.activate(
                    initialized, store, assignment, acquiring, leases, () -> 1);
            assertEquals(ShardLifecycleState.ACTIVE_FOR_COMMANDS, active.state());
            assertEquals(active.ownerEpoch(), store.runtimeMetadata().lastOpenedOwnerEpoch());
            reopenOwner[0] = new ReopenOwner(
                    assignment,
                    leases,
                    active.ownerEpoch(),
                    loseTransitionResponse,
                    loseReleaseResponse,
                    throwTransitionAfterCommit,
                    throwReleaseAfterCommit);
            final var workerClasses = workClasses();
            TargetClaimRecord claim = null;
            if (claimed) {
                final var actualQueue = TargetQueueState.decode(TargetValueEnvelope.decode(
                                store.get(ColumnFamily.META, TargetKeyCodec.state(binding.target())),
                                TargetQueueState.VALUE_TYPE)
                        .payload());
                final var expectedSiblingCommand =
                        new java.util.concurrent.atomic.AtomicReference<PreparedCommand>();
                final var claimRuntime = new TargetSourceApplyRuntime(
                        initialized,
                        store,
                        assignment,
                        active,
                        new TargetSourceApplyRuntime.Authorities(
                                leases,
                                SourceReplaySuccessor.strictKafka(),
                                entry -> {
                                    throw new AssertionError("Claim resolved a grant");
                                },
                                entry -> {
                                    throw new AssertionError("Claim resolved a fence");
                                },
                                entry -> { throw new AssertionError("unexpected Target expiry authority"); },
                                entry -> {
                                    throw new AssertionError("Claim resolved a Close");
                                },
                                entry -> {
                                    throw new AssertionError("Claim resolved membership");
                                },
                                (a, b, c) -> guard(),
                                (a, b) -> guard(),
                                entry -> {
                                    final var expected = expectedSiblingCommand.get();
                                    if (expected == null || !expected.equals(entry.command())) {
                                        throw new AssertionError("Claim resolved an unexpected command");
                                    }
                                    return new TargetSourceApplyRuntime.CommandControl(
                                            initialPolicy,
                                            (reader, bound, source) -> false,
                                            (reader, bound) -> java.util.Optional.empty(),
                                            scheduleProvider,
                                            noProofs(),
                                            (a, b, c) -> guard());
                                }),
                        new TargetSourceApplyRuntime.Limits(4096, 32L << 20, 60_000_000_000L, 16, 1),
                        System::nanoTime);
                final var wrongAssignment = new SourceAssignment(
                        assignment.shardId(),
                        bytes(32, 0x7d),
                        assignment.assignmentEpoch() + 1,
                        assignment.activationBarrier());
                final var wrongSourceClosed = new java.util.concurrent.atomic.AtomicBoolean();
                final SourceRecordConsumer wrongSource = new SourceRecordConsumer() {
                    @Override
                    public java.util.Optional<PolledSourceRecord> poll() {
                        return java.util.Optional.empty();
                    }

                    @Override
                    public void close() {
                        wrongSourceClosed.set(true);
                    }
                };
                final var maintenance = new TargetWorkerShardRuntime.Maintenance(
                        new TargetCloseStore(backend, scope, lineage, 16, 1)
                                .reservationControls((reader, bound) -> java.util.Optional.empty()),
                        new TargetReservationClosureWorkClassExecutor.Limits(4096, 250_000, 60_000_000_000L),
                        new TargetReservationExpiryWorkClassExecutor.Limits(2048, 100_000, 60_000_000_000L),
                        (a, b, c) -> guard(),
                        ignored -> {},
                        ignored -> {},
                        ignored -> {},
                        () -> 100);
                assertThrows(
                        IllegalArgumentException.class,
                        () -> TargetWorkerShardFactory.create(
                                wrongSource,
                                wrongAssignment,
                                workerClasses,
                                store,
                                store.sharedResources(),
                                claimRuntime,
                                maintenance));
                assertFalse(wrongSourceClosed.get());
                try (var wrongResources = WorkerRuntimeTestSupport.openWithSyntheticObservation(config)) {
                    assertThrows(
                            IllegalArgumentException.class,
                            () -> TargetWorkerShardFactory.create(
                                    wrongSource,
                                    assignment,
                                    workerClasses,
                                    store,
                                    wrongResources,
                                    claimRuntime,
                                    maintenance));
                    assertTrue(wrongSourceClosed.get());
                }
                final var claimNextSourceRecord = new java.util.concurrent.atomic.AtomicReference<
                        SourceRecordConsumer.PolledSourceRecord>();
                final var claimWorker = TargetWorkerShardFactory.create(
                        () -> java.util.Optional.ofNullable(claimNextSourceRecord.getAndSet(null)),
                        claimRuntime.acceptedAssignment(),
                        workerClasses,
                        store,
                        store.sharedResources(),
                        claimRuntime,
                        maintenance);
                final var actualOwner =
                        new OwnerIdentity(bytes(16, 0x72), bytes(16, 0x73), active.ownerEpoch(), active.leaseToken());
                final long beforeScan = store.latestSequenceNumber();
                final var incompletePage = claimWorker.scanTargetQueues(
                        new BoundedReadBudget(1, 32L << 20, 60_000_000_000L, System::nanoTime), null, 1, () -> 100);
                assertEquals(TargetQueueSnapshotReader.Stop.READ_BUDGET, incompletePage.stop());
                assertTrue(incompletePage.entries().isEmpty());
                final var partialPage = claimWorker.scanTargetQueues(
                        new BoundedReadBudget(3, 32L << 20, 60_000_000_000L, System::nanoTime), null, 2, () -> 100);
                assertEquals(TargetQueueSnapshotReader.Stop.READ_BUDGET, partialPage.stop());
                assertEquals(1, partialPage.entries().size());
                assertEquals(partialPage.entries().getFirst().queue().targetId(), partialPage.nextAfter());
                assertEquals(beforeScan, partialPage.cut().nativeSequence());
                final var scannedTargets = new HashSet<TargetPartitionId>();
                TargetPartitionId after = null;
                TargetQueueSnapshotReader.Cut scanCut = null;
                boolean scanComplete = false;
                for (int pageIndex = 0; pageIndex < 8; pageIndex++) {
                    final var page = claimWorker.scanTargetQueues(budget(), after, 1, () -> 100);
                    if (scanCut == null) {
                        scanCut = page.cut();
                    } else {
                        assertEquals(scanCut, page.cut());
                    }
                    for (var entry : page.entries()) {
                        assertTrue(scannedTargets.add(entry.queue().targetId()));
                        if (entry.queue().targetId().equals(binding.target())) {
                            assertEquals(physical, entry.physical());
                            assertEquals(actualQueue, entry.queue());
                        }
                    }
                    if (page.complete()) {
                        scanComplete = true;
                        break;
                    }
                    assertEquals(TargetQueueSnapshotReader.Stop.PAGE_LIMIT, page.stop());
                    after = page.nextAfter();
                }
                assertTrue(scanComplete);
                assertTrue(scannedTargets.contains(binding.target()));
                assertEquals(scanCut, claimWorker.readTargetQueueCut(budget(), () -> 100));
                assertEquals(beforeScan, store.latestSequenceNumber());
                assertThrows(
                        com.nereusstream.delay.store.ReadIncompleteException.class,
                        () -> claimWorker.readTargetQueue(
                                new BoundedReadBudget(1, 32L << 20, 60_000_000_000L, System::nanoTime),
                                binding.target(),
                                () -> 100));
                assertEquals(
                        actualQueue,
                        claimWorker
                                .readTargetQueue(budget(), binding.target(), () -> 100)
                                .orElseThrow()
                                .queue());
                assertTrue(claimWorker
                        .readTargetQueue(budget(), new TargetPartitionId(bytes(32, 0x7e)), () -> 100)
                        .isEmpty());
                final var selectedHead = actualQueue.domains().getFirst().ordinaryHead();
                assertThrows(
                        com.nereusstream.delay.store.ReadIncompleteException.class,
                        () -> claimWorker.probeSelectedHead(
                                new BoundedReadBudget(1, 32L << 20, 60_000_000_000L, System::nanoTime),
                                selectedHead,
                                () -> 100));
                final var headCost = claimWorker.probeSelectedHead(budget(), selectedHead, () -> 100);
                assertEquals(selectedHead, headCost.head());
                assertEquals(actualQueue, headCost.queue());
                assertEquals(claimExecutionBytes, headCost.executionBytes());
                assertEquals(
                        originalGrant
                                .accounting()
                                .schedulingCost(
                                        intent.adapterMetadata().kind()
                                                        == com.nereusstream.delay.protocol.AdapterMetadata.Kind.KAFKA
                                                ? AdapterKind.KAFKA
                                                : AdapterKind.PULSAR,
                                        message.payloadLength(),
                                        intent.adapterMetadata().canonicalBytes().length),
                        headCost.schedulingCost());
                final var nativeHead = actualQueue.domains().getFirst().nativeHead();
                assertNull(
                        nativeHead,
                        "a seeded Native scope without source-applied approval must use ordinary delivery");
                assertEquals(beforeScan, store.latestSequenceNumber());
                final long beforeClaim = store.latestSequenceNumber();
                assertThrows(
                        IllegalStateException.class,
                        () -> claimWorker.claim(
                                budget(),
                                actualQueue.domains().getFirst().ordinaryHead(),
                                new OwnerIdentity(
                                        bytes(16, 0x72), bytes(16, 0x73), active.ownerEpoch(), bytes(32, 0x74)),
                                message.deliverAtEpochMs(),
                                message.deliverAtEpochMs() + 1000,
                                claimExecutionBytes,
                                bytes(32, 0x71),
                                (kind, delta) -> {},
                                (a, b, c) -> guard(),
                                () -> 100));
                assertEquals(beforeClaim, store.latestSequenceNumber());
                for (long incorrectBytes : new long[] {claimExecutionBytes - 1, claimExecutionBytes + 1}) {
                    assertThrows(
                            IllegalArgumentException.class,
                            () -> claimWorker.claim(
                                    budget(),
                                    actualQueue.domains().getFirst().ordinaryHead(),
                                    actualOwner,
                                    message.deliverAtEpochMs(),
                                    message.deliverAtEpochMs() + 1000,
                                    incorrectBytes,
                                    bytes(32, 0x71),
                                    (kind, delta) -> {},
                                    (a, b, c) -> guard(),
                                    () -> 100));
                    assertEquals(beforeClaim, store.latestSequenceNumber());
                }
                assertThrows(
                        IllegalStateException.class,
                        () -> claimWorker.claim(
                                budget(),
                                actualQueue.domains().getFirst().ordinaryHead(),
                                actualOwner,
                                message.deliverAtEpochMs(),
                                message.deliverAtEpochMs() + 1000,
                                claimExecutionBytes,
                                bytes(32, 0x71),
                                (kind, delta) -> {},
                                (a, b, c) -> {
                                    throw new IllegalStateException("Claim write authority unavailable");
                                },
                                () -> 100));
                assertEquals(beforeClaim, store.latestSequenceNumber());
                final var otherShard = new ShardId(
                        scope.shard().routeIncarnation(), scope.shard().partition() + 1);
                final var otherScope = new TargetQuotaScope(otherShard, scope.tenantScope(), null);
                final var otherSource = new KafkaSourcePosition(
                        otherShard,
                        origin.authenticatedClusterId(),
                        origin.nativeTopicUuid(),
                        origin.offset(),
                        origin.leaderEpoch(),
                        origin.brokerLogAppendTimeEpochMs());
                final var otherGrantRequest = new TargetQuotaGrantControlRequest(
                        new TargetQuotaGrant(
                                otherScope,
                                bytes(32, 0x81),
                                1,
                                originalGrant.accounting(),
                                new TargetQuotaUsage(new CapacityVector(amounts), 64, 64, 64, 64),
                                originalGrant.tenantPolicyVersion(),
                                originalGrant.tenantPolicyHash()),
                        null,
                        null);
                final var otherSigned = signed(otherGrantRequest, bytes(32, 0x82), actor, keys);
                registrations.register(otherSigned.control());
                try (var otherStore = ShardStore.openTarget(config, otherShard, resources)) {
                    final var otherInitialized = TargetStoreBootstrap.commit(
                            TargetStoreBootstrap.prepare(
                                    otherStore,
                                    otherScope,
                                    bytes(16, 0xcd),
                                    new TargetStoreBackend.WriteLimits(64, 2 << 20),
                                    budget(),
                                    otherSigned.control(),
                                    otherSigned.mutation(),
                                    otherSource,
                                    authority(
                                            registrations,
                                            keys,
                                            actor,
                                            otherSource,
                                            otherGrantRequest,
                                            (a, b, c, d) -> {}),
                                    (a, b, c) -> {}),
                            (a, b, c) -> guard());
                    final var otherTargetGrantRequest = new TargetQuotaGrantControlRequest(
                            new TargetQuotaGrant(
                                    otherScope.forTarget(physical.id()),
                                    ownerSession(0x85),
                                    1,
                                    originalGrant.accounting(),
                                    new TargetQuotaUsage(new CapacityVector(otherTargetAmounts), 1, 64, 64, 64),
                                    originalGrant.tenantPolicyVersion(),
                                    originalGrant.tenantPolicyHash()),
                            null,
                            null);
                    final var otherTargetSigned = signed(otherTargetGrantRequest, bytes(32, 0x86), actor, keys);
                    registrations.register(otherTargetSigned.control());
                    final var otherGrantAt =
                            source(otherSource, otherSource.offset() + 1, otherSource.brokerLogAppendTimeEpochMs() + 1);
                    final var otherBackend = otherInitialized.backend();
                    final var otherLineage = otherInitialized.root().recoveryLineage();
                    final var otherGrants = new TargetQuotaGrantStore(otherBackend, otherScope, otherLineage, 16, 1);
                    assertEquals(
                            StableCode.OK,
                            otherGrants
                                    .commit(
                                            otherGrants.prepareFirst(
                                                    budget(),
                                                    otherTargetSigned.control(),
                                                    otherTargetSigned.mutation(),
                                                    otherGrantAt,
                                                    authority(
                                                            registrations,
                                                            keys,
                                                            actor,
                                                            otherGrantAt,
                                                            otherTargetGrantRequest,
                                                            (a, b, c, d) -> {})),
                                            (a, b, c) -> guard())
                                    .stableCode());
                    final var otherActivation = TargetQuotaGrantActivation.decode(TargetValueEnvelope.decode(
                                    otherStore.get(
                                            ColumnFamily.META,
                                            Bytes.concat(
                                                    new byte[] {TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG, 1},
                                                    otherTargetGrantRequest
                                                            .next()
                                                            .scope()
                                                            .keySuffix())),
                                    TargetQuotaGrantActivation.VALUE_TYPE)
                            .payload());
                    final var otherQueue = new TargetQueueState(
                            physical.id(),
                            1,
                            1,
                            TargetQueueState.AdmissionState.OPEN,
                            otherActivation.allocation().identity().accountingIncarnation(),
                            0,
                            List.of());
                    final var otherQueueAt = source(
                            otherGrantAt, otherGrantAt.offset() + 1, otherGrantAt.brokerLogAppendTimeEpochMs() + 1);
                    new TargetMessageStore(otherBackend, 1, 1, 1)
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
                                                            TargetKeyCodec.state(physical.id()),
                                                            TargetQueueState.VALUE_TYPE,
                                                            otherQueue.canonicalBytes()))),
                                    new TargetSourceAccounting(
                                            otherScope,
                                            otherLineage,
                                            otherQueueAt,
                                            Bytes.sha256(Bytes.utf8("other-shard-target-queue-fixture")),
                                            16,
                                            1,
                                            1),
                                    (a, b, c) -> guard());
                    final var otherControls =
                            new TargetControlScope(physical.id(), otherShard, controls.controls(), controls.permits());
                    final var otherMembershipPolicy = new TargetMembershipPolicy(
                            otherScope.tenantScope(),
                            destination.ref(),
                            dispatch.digest(),
                            dispatch,
                            otherControls,
                            bytes(32, 0x87));
                    final var otherMembershipAt = source(
                            otherQueueAt, otherQueueAt.offset() + 1, otherQueueAt.brokerLogAppendTimeEpochMs() + 1);
                    final var otherMembership = new TargetMembershipGrant(
                            otherScope.tenantScope(),
                            destination.ref(),
                            dispatch,
                            dispatch,
                            otherControls,
                            otherMembershipPolicy.digest(),
                            bytes(32, 0x88),
                            bytes(32, 0x89),
                            otherMembershipAt);
                    new TargetMessageStore(otherBackend, 1, 1, 1)
                            .applyAccounted(
                                    budget(),
                                    reader -> new TargetMessageStore.Input(
                                            List.of(),
                                            List.of(),
                                            List.of(
                                                    reader.replace(
                                                            ColumnFamily.META,
                                                            otherMembershipPolicy.encodedKey(),
                                                            TargetMembershipPolicy.VALUE_TYPE,
                                                            otherMembershipPolicy.canonicalBytes()),
                                                    reader.replace(
                                                            ColumnFamily.META,
                                                            otherMembership.encodedKey(),
                                                            TargetMembershipGrant.VALUE_TYPE,
                                                            otherMembership.canonicalBytes()))),
                                    new TargetSourceAccounting(
                                            otherScope,
                                            otherLineage,
                                            otherMembershipAt,
                                            otherMembership.sourceMutationDigest(),
                                            16,
                                            1,
                                            1),
                                    (a, b, c) -> guard());
                    final var otherScheduleAt = source(
                            otherMembershipAt,
                            otherMembershipAt.offset() + 1,
                            otherMembershipAt.brokerLogAppendTimeEpochMs() + 1);
                    final byte[] otherOrderingDomain = uncertainRetry ? null : bytes(32, 0x8a);
                    final var otherRetryPolicy = targetOutcomeRetryPolicy(uncertainRetry);
                    final var otherIntent = CanonicalScheduleIntent.create(
                            destination.ref(),
                            otherRetryPolicy.ref(),
                            otherScheduleAt.brokerLogAppendTimeEpochMs() + 100,
                            uncertainRetry
                                    ? Math.max(message.deliverAtEpochMs(),
                                            otherScheduleAt.brokerLogAppendTimeEpochMs() + 100) + 4000
                                    : otherScheduleAt.brokerLogAppendTimeEpochMs() + 2000,
                            priorIntent.deliveryMode(),
                            uncertainRetry ? OrderingMode.BEST_EFFORT : OrderingMode.DELIVERY_TIME_FIFO,
                            uncertainRetry ? new byte[0] : Bytes.utf8("target-admission-order-key"),
                            model.inlinePayload(),
                            null,
                            priorIntent.adapterMetadata(),
                            priorIntent.businessKey(),
                            priorIntent.eventTimeEpochMs(),
                            NativeDeliveryPolicy.FORBID);
                    final var otherSeed = new DelayMessageId(SelfRoutingId.fromLogicalUuid(
                                    otherShard, initial.messageId().routingId().logicalId())
                            .bytes());
                    final var otherSchedule = schedule(otherIntent, otherSeed, otherScheduleAt, 42);
                    final var otherBinding = new TargetScheduleBinding(
                            otherSchedule.delayMessageId(),
                            CommandType.SCHEDULE,
                            otherSchedule.canonicalBody(),
                            otherScheduleAt,
                            physical.id(),
                            initial.domain(),
                            otherActivation.allocation().identity().accountingIncarnation(),
                            dispatch.digest(),
                            dispatch.digest(),
                            otherControls.digest(),
                            otherMembership.digest(),
                            null,
                            otherOrderingDomain);
                    final var otherProfiles = ProfileBindingControlState.empty()
                            .activate(destination.ref(), otherSource)
                            .activate(capability.ref(), otherGrantAt);
                    final var otherAuthority = new TargetScheduleRegistration.Authority(
                            otherBinding, physical, destination, capability, otherProfiles, 60000);
                    final var otherCommands = new TargetCommandStore(otherBackend, otherScope, otherLineage, 16, 1);
                    final var otherPolicy = new TargetCommandStore.Policy(
                            otherScope,
                            1000,
                            1000,
                            10,
                            java.util.Set.of(otherSchedule.protocolTuple()),
                            new TargetCommandStore.DeliveryWindow(10_000, 1, 100_000));
                    assertEquals(
                            StableCode.SCHEDULED,
                            otherCommands
                                    .commit(
                                            otherCommands.prepareFirst(
                                                    budget(),
                                                    otherSchedule,
                                                    otherScheduleAt,
                                                    otherPolicy,
                                                    (reader, bound, source) -> false,
                                                    (reader, bound) -> java.util.Optional.empty(),
                                                    (incoming, source) -> new TargetCommandStore.ScheduleAdmission(
                                                            StableCode.OK,
                                                            otherAuthority,
                                                            TargetOrderState.OrderingContract.ADMISSION_WATERMARK),
                                                    noProofs()),
                                            (a, b, c) -> guard())
                                    .stableCode());
                    final var otherMessage = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                                    otherStore.get(
                                            ColumnFamily.ID, TargetKeyCodec.message(otherSchedule.delayMessageId())),
                                    TargetMessageRecord.VALUE_TYPE)
                            .payload());
                    final long ordinaryClaimNow = Math.max(message.deliverAtEpochMs(), otherMessage.deliverAtEpochMs());
                    final long otherTargetDeliverAt = Math.addExact(ordinaryClaimNow, 1000);
                    final var otherPhysical = new CanonicalTargetPartition(
                            physical.resource(), Math.addExact(physical.physicalPartition(), 1));
                    final var otherPhysicalGrantRequest = new TargetQuotaGrantControlRequest(
                            new TargetQuotaGrant(
                                    otherScope.forTarget(otherPhysical.id()),
                                    bytes(32, 0x9a),
                                    1,
                                    originalGrant.accounting(),
                                    new TargetQuotaUsage(new CapacityVector(targetAmounts), 1, 64, 64, 64),
                                    originalGrant.tenantPolicyVersion(),
                                    originalGrant.tenantPolicyHash()),
                            null,
                            null);
                    final var otherTargetGrantSigned = signed(otherPhysicalGrantRequest, bytes(32, 0x9b), actor, keys);
                    registrations.register(otherTargetGrantSigned.control());
                    final var otherTargetGrantAt = source(
                            otherScheduleAt,
                            otherScheduleAt.offset() + 1,
                            otherScheduleAt.brokerLogAppendTimeEpochMs() + 1);
                    final var otherPhysicalGrants = new TargetQuotaGrantStore(
                            otherBackend, otherScope, otherLineage, 16, 1);
                    assertEquals(
                            StableCode.OK,
                            otherPhysicalGrants
                                    .commit(
                                            otherPhysicalGrants.prepareFirst(
                                                    budget(),
                                                    otherTargetGrantSigned.control(),
                                                    otherTargetGrantSigned.mutation(),
                                                    otherTargetGrantAt,
                                                    authority(
                                                            registrations,
                                                            keys,
                                                            actor,
                                                            otherTargetGrantAt,
                                                            otherPhysicalGrantRequest,
                                                            (a, b, c, d) -> {})),
                                            (a, b, c) -> guard())
                                    .stableCode());
                    final var otherPhysicalGrantActivation = TargetQuotaGrantActivation.decode(
                            TargetValueEnvelope.decode(
                                    otherStore.get(
                                            ColumnFamily.META,
                                            Bytes.concat(
                                                    new byte[] {TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG, 1},
                                                    otherPhysicalGrantRequest.next().scope().keySuffix())),
                                    TargetQuotaGrantActivation.VALUE_TYPE)
                            .payload());
                    final var otherPhysicalControls = new TargetControlScope(
                            otherPhysical.id(), otherShard, controls.controls(), controls.permits());
                    final var otherDispatch = TargetDispatchCompatibility.fromProfiles(
                            otherPhysical, destination, capability);
                    final var otherTargetMembershipPolicy = new TargetMembershipPolicy(
                            otherScope.tenantScope(),
                            destination.ref(),
                            otherDispatch.digest(),
                            otherDispatch,
                            otherPhysicalControls,
                            bytes(32, 0x9c));
                    final var otherTargetMembershipAt = source(
                            otherTargetGrantAt,
                            otherTargetGrantAt.offset() + 1,
                            otherTargetGrantAt.brokerLogAppendTimeEpochMs() + 1);
                    final var otherTargetMembership = new TargetMembershipGrant(
                            otherScope.tenantScope(),
                            destination.ref(),
                            otherDispatch,
                            otherDispatch,
                            otherPhysicalControls,
                            otherTargetMembershipPolicy.digest(),
                            bytes(32, 0x9d),
                            bytes(32, 0x9e),
                            otherTargetMembershipAt);
                    final var otherTargetQueue = new TargetQueueState(
                            otherPhysical.id(),
                            1,
                            1,
                            TargetQueueState.AdmissionState.OPEN,
                            otherPhysicalGrantActivation.allocation().identity().accountingIncarnation(),
                            0,
                            List.of());
                    new TargetMessageStore(otherBackend, 1, 1, 1)
                            .applyAccounted(
                                    budget(),
                                    reader -> new TargetMessageStore.Input(
                                            List.of(),
                                            List.of(),
                                            List.of(
                                                    reader.replace(
                                                            ColumnFamily.META,
                                                            TargetKeyCodec.identity(otherPhysical.id()),
                                                            CanonicalTargetPartition.VALUE_TYPE,
                                                            otherPhysical.canonicalBytes()),
                                                    reader.replace(
                                                            ColumnFamily.META,
                                                            TargetKeyCodec.state(otherPhysical.id()),
                                                            TargetQueueState.VALUE_TYPE,
                                                            otherTargetQueue.canonicalBytes()),
                                                    reader.replace(
                                                            ColumnFamily.META,
                                                            otherDispatch.encodedKey(),
                                                            TargetDispatchCompatibility.VALUE_TYPE,
                                                            otherDispatch.canonicalBytes()),
                                                    reader.replace(
                                                            ColumnFamily.META,
                                                            otherPhysicalControls.encodedKey(),
                                                            TargetControlScope.VALUE_TYPE,
                                                            otherPhysicalControls.canonicalBytes()),
                                                    reader.replace(
                                                            ColumnFamily.META,
                                                            otherTargetMembershipPolicy.encodedKey(),
                                                            TargetMembershipPolicy.VALUE_TYPE,
                                                            otherTargetMembershipPolicy.canonicalBytes()),
                                                    reader.replace(
                                                            ColumnFamily.META,
                                                            otherTargetMembership.encodedKey(),
                                                            TargetMembershipGrant.VALUE_TYPE,
                                                            otherTargetMembership.canonicalBytes()))),
                                    new TargetSourceAccounting(
                                            otherScope,
                                            otherLineage,
                                            otherTargetMembershipAt,
                                            otherTargetMembership.sourceMutationDigest(),
                                            16,
                                            1,
                                            1),
                                    (a, b, c) -> guard());
                    final var otherTargetScheduleAt = source(
                            otherTargetMembershipAt,
                            otherTargetMembershipAt.offset() + 1,
                            otherTargetMembershipAt.brokerLogAppendTimeEpochMs() + 1);
                    final var otherTargetIntent = CanonicalScheduleIntent.create(
                            destination.ref(),
                            priorIntent.retryPolicy(),
                            otherTargetDeliverAt,
                            otherTargetDeliverAt + 2000,
                            priorIntent.deliveryMode(),
                            priorIntent.orderingMode(),
                            priorIntent.orderingKey(),
                            model.inlinePayload(),
                            null,
                            priorIntent.adapterMetadata(),
                            priorIntent.businessKey(),
                            priorIntent.eventTimeEpochMs(),
                            NativeDeliveryPolicy.FORBID);
                    final var otherTargetSchedule = schedule(
                            otherTargetIntent, otherSchedule.delayMessageId(), otherTargetScheduleAt, 43);
                    final var otherTargetBinding = new TargetScheduleBinding(
                            otherTargetSchedule.delayMessageId(),
                            CommandType.SCHEDULE,
                            otherTargetSchedule.canonicalBody(),
                            otherTargetScheduleAt,
                            otherPhysical.id(),
                            initial.domain(),
                            otherPhysicalGrantActivation.allocation().identity().accountingIncarnation(),
                            otherDispatch.digest(),
                            otherDispatch.digest(),
                            otherPhysicalControls.digest(),
                            otherTargetMembership.digest(),
                            null,
                            null);
                    final var otherTargetAuthority = new TargetScheduleRegistration.Authority(
                            otherTargetBinding, otherPhysical, destination, capability, otherProfiles, 60000);
                    assertEquals(
                            StableCode.SCHEDULED,
                            otherCommands
                                    .commit(
                                            otherCommands.prepareFirst(
                                                    budget(),
                                                    otherTargetSchedule,
                                                    otherTargetScheduleAt,
                                                    otherPolicy,
                                                    (reader, bound, source) -> false,
                                                    (reader, bound) -> java.util.Optional.empty(),
                                                    (incoming, source) -> new TargetCommandStore.ScheduleAdmission(
                                                            StableCode.OK,
                                                            otherTargetAuthority,
                                                            TargetOrderState.OrderingContract.ADMISSION_WATERMARK),
                                                    noProofs()),
                                            (a, b, c) -> guard())
                                    .stableCode());
                    final var otherFollowerScheduleAt = source(
                            otherTargetScheduleAt,
                            otherTargetScheduleAt.offset() + 1,
                            otherTargetScheduleAt.brokerLogAppendTimeEpochMs() + 1);
                    final long otherFollowerDeliverAt = uncertainRetry
                            ? Math.max(otherIntent.deliverAtEpochMs(), otherTargetDeliverAt) + 1_000
                            : otherIntent.deliverAtEpochMs() + 1_000;
                    final var otherFollowerIntent = CanonicalScheduleIntent.create(
                            destination.ref(),
                            otherIntent.retryPolicy(),
                            otherFollowerDeliverAt,
                            otherFollowerDeliverAt + 2_000,
                            otherIntent.deliveryMode(),
                            otherIntent.orderingMode(),
                            otherIntent.orderingKey(),
                            model.inlinePayload(),
                            null,
                            otherIntent.adapterMetadata(),
                            otherIntent.businessKey(),
                            otherIntent.eventTimeEpochMs(),
                            NativeDeliveryPolicy.FORBID);
                    final var otherFollowerSchedule = schedule(
                            otherFollowerIntent, otherSeed, otherFollowerScheduleAt, 45);
                    final var otherFollowerBinding = new TargetScheduleBinding(
                            otherFollowerSchedule.delayMessageId(),
                            CommandType.SCHEDULE,
                            otherFollowerSchedule.canonicalBody(),
                            otherFollowerScheduleAt,
                            physical.id(),
                            initial.domain(),
                            otherActivation.allocation().identity().accountingIncarnation(),
                            dispatch.digest(),
                            dispatch.digest(),
                            otherControls.digest(),
                            otherMembership.digest(),
                            null,
                            otherOrderingDomain);
                    final var otherFollowerAuthority = new TargetScheduleRegistration.Authority(
                            otherFollowerBinding, physical, destination, capability, otherProfiles, 60000);
                    assertEquals(
                            StableCode.SCHEDULED,
                            otherCommands
                                    .commit(
                                            otherCommands.prepareFirst(
                                                    budget(),
                                                    otherFollowerSchedule,
                                                    otherFollowerScheduleAt,
                                                    otherPolicy,
                                                    (reader, bound, source) -> false,
                                                    (reader, bound) -> java.util.Optional.empty(),
                                                    (incoming, source) -> new TargetCommandStore.ScheduleAdmission(
                                                            StableCode.OK,
                                                            otherFollowerAuthority,
                                                            TargetOrderState.OrderingContract.ADMISSION_WATERMARK),
                                                    noProofs()),
                                            (a, b, c) -> guard())
                                    .stableCode());
                    final var otherAssignment = new SourceAssignment(
                            otherShard,
                            bytes(32, 0x83),
                            1,
                            new KafkaActivationBarrier(
                                    otherShard,
                                    otherSource.authenticatedClusterId(),
                                    otherSource.nativeTopicUuid(),
                                    otherSource.offset() + 1));
                    final var otherActive = leases.transition(
                                    leases.acquire(otherAssignment, "other-claim-worker", ownerSession(0x84), 1, 10000)
                                            .orElseThrow(),
                                    ShardLifecycleState.ACTIVE_FOR_COMMANDS)
                            .orElseThrow();
                    otherStore.recordOpenedOwnerEpoch(otherActive.ownerEpoch());
                    final var otherRuntime = new TargetSourceApplyRuntime(
                            otherInitialized,
                            otherStore,
                            otherAssignment,
                            otherActive,
                            new TargetSourceApplyRuntime.Authorities(
                                    leases,
                                    SourceReplaySuccessor.strictKafka(),
                                    entry -> {
                                        throw new AssertionError("other Shard resolved a grant");
                                    },
                                    entry -> {
                                        throw new AssertionError("other Shard resolved a fence");
                                    },
                                    entry -> { throw new AssertionError("unexpected Target expiry authority"); },
                                    entry -> {
                                        throw new AssertionError("other Shard resolved a Close");
                                    },
                                    entry -> {
                                        throw new AssertionError("other Shard resolved membership");
                                    },
                                    (a, b, c) -> guard(),
                                    (a, b) -> guard(),
                                    entry -> {
                                        throw new AssertionError("other Shard resolved a command");
                                    }),
                            new TargetSourceApplyRuntime.Limits(4096, 32L << 20, 60_000_000_000L, 16, 1),
                            System::nanoTime);
                    final var otherWorker = TargetWorkerShardFactory.create(
                            () -> java.util.Optional.empty(),
                            otherRuntime.acceptedAssignment(),
                            workerClasses,
                            otherStore,
                            resources,
                            otherRuntime,
                            new TargetWorkerShardRuntime.Maintenance(
                                    new TargetCloseStore(
                                                    otherInitialized.backend(),
                                                    otherScope,
                                                    otherInitialized.root().recoveryLineage(),
                                                    16,
                                                    1)
                                            .reservationControls((reader, bound) -> java.util.Optional.empty()),
                                    new TargetReservationClosureWorkClassExecutor.Limits(
                                            4096, 250_000, 60_000_000_000L),
                                    new TargetReservationExpiryWorkClassExecutor.Limits(2048, 100_000, 60_000_000_000L),
                                    (a, b, c) -> guard(),
                                    ignored -> {},
                                    ignored -> {},
                                    ignored -> {},
                                    () -> 100));
                    final var claimHost = TargetWorkerHostTestBridge.withoutMaintenanceTimer(
                            workerClasses, resources, List.of(claimWorker));
                    assertThrows(
                            IllegalStateException.class,
                            () -> TargetWorkerHostTestBridge.withoutMaintenanceTimer(
                                    workerClasses, resources, List.of(otherWorker, claimWorker)));
                    assertTrue(claimHost.targetQueueChangeRevision() > 0);
                    final var inventory = claimHost.rebuildTargetInventory(
                            new TargetWorkerTargetInventory.Limits(2, 16, 4, 8, 4096, 32L << 20, 60_000_000_000L),
                            () -> 100,
                            System::nanoTime);
                    assertEquals(TargetWorkerTargetInventory.Stop.COMPLETE, inventory.stop());
                    assertEquals(
                            java.util.Set.of(scope.shard()),
                            inventory.snapshot().cuts().keySet());
                    assertEquals(
                            List.of(scope.shard()),
                            inventory.snapshot().targets().stream()
                                    .filter(target -> target.id().equals(physical.id()))
                                    .findFirst()
                                    .orElseThrow()
                                    .sources()
                                    .stream()
                                    .map(TargetWorkerTargetInventory.Source::shard)
                                    .toList());
                    final var otherHead = otherWorker
                            .readTargetQueue(budget(), physical.id(), () -> 100)
                            .orElseThrow()
                            .queue()
                            .domains()
                            .getFirst()
                            .ordinaryHead();
                    final var otherTargetHead = otherWorker
                            .readTargetQueue(budget(), otherPhysical.id(), () -> 100)
                            .orElseThrow()
                            .queue()
                            .domains()
                            .getFirst()
                            .ordinaryHead();
                    final var otherHeadCost = otherWorker.probeSelectedHead(budget(), otherHead, () -> 100);
                    final var otherTargetHeadCost = otherWorker.probeSelectedHead(budget(), otherTargetHead, () -> 100);
                    final long schedulingCost = Math.max(
                            headCost.schedulingCost(),
                            Math.max(otherHeadCost.schedulingCost(), otherTargetHeadCost.schedulingCost()));
                    final var ordinary = claimHost.newOrdinaryDrr(
                            inventory,
                            new TargetWorkerOrdinaryDrr.Limits(
                                    schedulingCost,
                                    schedulingCost,
                                    schedulingCost,
                                    16,
                                    4096,
                                    32L << 20,
                                    60_000_000_000L),
                            () -> 100,
                            System::nanoTime);
                    final var claimBudget = new SchedulerBudget(1, schedulingCost, 60_000_000_000L);
                    final long claimNow = ordinaryClaimNow;
                    final var otherOwner = new OwnerIdentity(
                            bytes(16, 0x92), bytes(16, 0x93), otherActive.ownerEpoch(), otherActive.leaseToken());
                    final TargetWorkerOrdinaryDrr.Requests claimRequests = (shard, selected) -> {
                        final boolean first = shard == claimWorker;
                        assertTrue(first || shard == otherWorker);
                        if (first) {
                            assertEquals(selectedHead, selected.head());
                        } else {
                            assertTrue(selected.head().equals(otherHead) || selected.head().equals(otherTargetHead));
                        }
                        return java.util.Optional.of(new TargetWorkerOrdinaryDrr.Request(
                                first ? actualOwner : otherOwner,
                                Math.max(claimNow, selected.head().timeEpochMs()) + 1000,
                                bytes(32, first ? 0x71 : 0x91),
                                (kind, delta) -> {},
                                (a, b, c) -> guard()));
                    };
                    final var frozen = ordinary.freezeRecoveryFirstPass(claimNow, claimBudget, claimRequests);
                    assertEquals(TargetWorkerOrdinaryDrr.FreezeStop.READY, frozen.stop());
                    assertEquals(1, frozen.eligibleTargets());
                    final long firstBeforeUnavailable = store.latestSequenceNumber();
                    final long otherBeforeUnavailable = otherStore.latestSequenceNumber();
                    final TargetWorkerOrdinaryDrr.Requests unavailableRequests = (shard, selected) ->
                            java.util.Optional.empty();
                    assertTrue(ordinary.claimOrdinary(claimNow, claimBudget, unavailableRequests)
                            .claims()
                            .isEmpty());
                    assertEquals(firstBeforeUnavailable, store.latestSequenceNumber());
                    assertEquals(otherBeforeUnavailable, otherStore.latestSequenceNumber());
                    assertTrue(schedulingCost > 1);
                    assertTrue(ordinary.claimOrdinary(
                                            claimNow,
                                            new SchedulerBudget(1, schedulingCost - 1, 60_000_000_000L),
                                            claimRequests)
                            .claims()
                            .isEmpty());
                    assertEquals(firstBeforeUnavailable, store.latestSequenceNumber());
                    assertEquals(otherBeforeUnavailable, otherStore.latestSequenceNumber());
                    final long queueRevisionBeforeRejectedClaim = claimHost.targetQueueChangeRevision();
                    final long firstBeforeRejectedClaim = store.latestSequenceNumber();
                    assertThrows(
                            IllegalStateException.class,
                            () -> claimHost.claim(
                                    claimWorker,
                                    budget(),
                                    selectedHead,
                                    actualOwner,
                                    claimNow,
                                    claimNow + 1000,
                                    claimExecutionBytes,
                                    bytes(32, 0x70),
                                    (kind, delta) -> {},
                                    (a, b, c) -> {
                                        throw new IllegalStateException("Claim write authority unavailable");
                                    },
                                    () -> 100));
                    assertEquals(firstBeforeRejectedClaim, store.latestSequenceNumber());
                    assertEquals(queueRevisionBeforeRejectedClaim, claimHost.targetQueueChangeRevision());
                    assertFalse(claimHost.awaitTargetQueueChange(
                            queueRevisionBeforeRejectedClaim, java.time.Duration.ZERO));
                    final long otherBeforeClaim = otherStore.latestSequenceNumber();
                    final long queueRevisionBeforeClaim = claimHost.targetQueueChangeRevision();
                    claim = ordinary.claimOrdinary(claimNow, claimBudget, claimRequests)
                            .claims()
                            .getFirst();
                    assertEquals(selectedHead, claim.selected());
                    assertEquals(actualOwner, claim.owner());
                    assertEquals(otherBeforeClaim, otherStore.latestSequenceNumber());
                    assertEquals(queueRevisionBeforeClaim + 1, claimHost.targetQueueChangeRevision());
                    assertTrue(claimHost.awaitTargetQueueChange(
                            queueRevisionBeforeClaim, java.time.Duration.ZERO));
                    final long firstAfterClaim = store.latestSequenceNumber();
                    final long queueRevisionBeforeAdmission = claimHost.targetQueueChangeRevision();
                    claimHost.admitShard(otherWorker);
                    assertTrue(claimHost.targetQueueChangeRevision() > queueRevisionBeforeAdmission);
                    final var afterClaimInventory = claimHost.rebuildTargetInventory(
                            new TargetWorkerTargetInventory.Limits(2, 16, 4, 8, 4096, 32L << 20, 60_000_000_000L),
                            () -> 100,
                            System::nanoTime);
                    assertEquals(TargetWorkerTargetInventory.Stop.COMPLETE, afterClaimInventory.stop());
                    ordinary.refreshInventory(afterClaimInventory);
                    final long queueRevisionBeforeOtherClaim = claimHost.targetQueueChangeRevision();
                    final var otherClaim = ordinary.claimOrdinary(claimNow, claimBudget, claimRequests)
                            .claims()
                            .getFirst();
                    assertEquals(otherHead, otherClaim.selected());
                    assertEquals(otherOwner, otherClaim.owner());
                    assertEquals(queueRevisionBeforeOtherClaim + 1, claimHost.targetQueueChangeRevision());
                    assertEquals(firstAfterClaim, store.latestSequenceNumber());
                    assertTrue(otherStore.latestSequenceNumber() > otherBeforeClaim);
                    final long firstBeforeEmptyPoll = store.latestSequenceNumber();
                    final long otherBeforeEmptyPoll = otherStore.latestSequenceNumber();
                    assertTrue(ordinary.claimOrdinary(claimNow, claimBudget, claimRequests).claims().isEmpty());
                    assertEquals(firstBeforeEmptyPoll, store.latestSequenceNumber());
                    assertEquals(otherBeforeEmptyPoll, otherStore.latestSequenceNumber());
                    final var originalClaim = claim;
                    final long otherBeforeRevoke = otherStore.latestSequenceNumber();
                    final long sourceSequenceBeforeRevoke = store.shardMutationSequence();
                    final var sourceBeforeRevoke = store.appliedShardLogPosition();
                    final var schedulerClaims = new java.util.concurrent.LinkedBlockingQueue<TargetClaimRecord>();
                    final var schedulerClaimGate = new java.util.concurrent.Semaphore(0);
                    final long schedulerDeadline = claimNow;
                    final var schedulerEpoch = new java.util.concurrent.atomic.AtomicLong(
                            Math.max(0, schedulerDeadline - 100));
                    final var schedulerOwnerEpoch = new java.util.concurrent.atomic.AtomicLong(100);
                    final var replacementLoopOnly = new java.util.concurrent.atomic.AtomicBoolean();
                    final var replacementLoopShard = new java.util.concurrent.atomic.AtomicReference<
                            TargetWorkerShardRuntime>();
                    final var replacementLoopOwner = new java.util.concurrent.atomic.AtomicReference<>(otherOwner);
                    final TargetWorkerOrdinaryDrr.Requests retryClaimRequests = (shard, selected) -> {
                        if (replacementLoopOnly.get()) {
                            if (shard != replacementLoopShard.get()
                                    || !selected.head().target().equals(physical.id())) {
                                return java.util.Optional.empty();
                            }
                            return java.util.Optional.of(new TargetWorkerOrdinaryDrr.Request(
                                    replacementLoopOwner.get(),
                                    Math.addExact(
                                            Math.max(schedulerEpoch.get(), selected.head().timeEpochMs()), 1000),
                                    bytes(32, 0xa4),
                                    (kind, delta) -> {},
                                    (a, b, c) -> guard()));
                        }
                        return claimRequests.resolve(shard, selected)
                                .map(requestForClaim -> new TargetWorkerOrdinaryDrr.Request(
                                        requestForClaim.owner(),
                                        Math.addExact(
                                                Math.max(schedulerEpoch.get(), selected.head().timeEpochMs()), 1000),
                                        bytes(32, 0xa4),
                                        requestForClaim.quota(),
                                        requestForClaim.physicalWrites()));
                    };
                    final var ordinaryInventoryLimits = new TargetWorkerTargetInventory.Limits(
                            2, 16, 4, 8, 4096, 32L << 20, 60_000_000_000L);
                    final var ordinaryDrrLimits = new TargetWorkerOrdinaryDrr.Limits(
                            schedulingCost,
                            schedulingCost,
                            schedulingCost,
                            16,
                            4096,
                            32L << 20,
                            60_000_000_000L);
                    assertThrows(
                            IllegalArgumentException.class,
                            () -> claimHost.startOrdinaryScheduling(
                                    new TargetWorkerTargetInventory.Limits(
                                            2, 8, 4, 8, 4096, 32L << 20, 60_000_000_000L),
                                    ordinaryDrrLimits,
                                    new SchedulerBudget(1, schedulingCost - 1, 60_000_000_000L),
                                    java.time.Duration.ofSeconds(10),
                                    retryClaimRequests,
                                    ignored -> {},
                                    () -> 100,
                                    schedulerEpoch::get,
                                    System::nanoTime,
                                    ignored -> {}));
                    assertThrows(
                            IllegalArgumentException.class,
                            () -> claimHost.startOrdinaryScheduling(
                                    new TargetWorkerTargetInventory.Limits(
                                            2, 12, 4, 8, 4096, 32L << 20, 60_000_000_000L),
                                    ordinaryDrrLimits,
                                    claimBudget,
                                    java.time.Duration.ZERO,
                                    retryClaimRequests,
                                    ignored -> {},
                                    () -> 100,
                                    schedulerEpoch::get,
                                    System::nanoTime,
                                    ignored -> {}));
                    assertThrows(
                            NullPointerException.class,
                            () -> claimHost.startOrdinaryScheduling(
                                    new TargetWorkerTargetInventory.Limits(
                                            2, 14, 4, 8, 4096, 32L << 20, 60_000_000_000L),
                                    ordinaryDrrLimits,
                                    claimBudget,
                                    java.time.Duration.ofSeconds(10),
                                    null,
                                    ignored -> {},
                                    () -> 100,
                                    schedulerEpoch::get,
                                    System::nanoTime,
                                    ignored -> {}));
                    final TargetWorkerOrdinaryDrr.Requests failingSubscription =
                            new TargetWorkerOrdinaryDrr.Requests() {
                                @Override
                                public java.util.Optional<TargetWorkerOrdinaryDrr.Request> resolve(
                                        TargetWorkerShardRuntime shard,
                                        com.nereusstream.delay.runtime.TargetHeadCostProbe.Cost cost) {
                                    return java.util.Optional.empty();
                                }

                                @Override
                                public java.io.Closeable subscribeNativePolicyChanges(final Runnable wakeup) {
                                    throw new IllegalStateException("Native policy subscription unavailable");
                                }
                            };
                    assertThrows(
                            IllegalStateException.class,
                            () -> claimHost.startOrdinaryScheduling(
                                    new TargetWorkerTargetInventory.Limits(
                                            2, 15, 4, 8, 4096, 32L << 20, 60_000_000_000L),
                                    ordinaryDrrLimits,
                                    claimBudget,
                                    java.time.Duration.ofSeconds(10),
                                    failingSubscription,
                                    ignored -> {},
                                    () -> 100,
                                    schedulerEpoch::get,
                                    System::nanoTime,
                                    ignored -> {}));
                    final long monotonicAnchor = System.nanoTime();
                    final java.util.function.LongSupplier negativeMonotonicClock =
                            () -> -1_000_000_000_000L + (System.nanoTime() - monotonicAnchor);
                    final var ordinaryLoop = claimHost.startOrdinaryScheduling(
                            ordinaryInventoryLimits,
                            ordinaryDrrLimits,
                            claimBudget,
                            java.time.Duration.ofSeconds(10),
                            retryClaimRequests,
                            scheduledClaim -> {
                                schedulerClaims.add(scheduledClaim);
                                try {
                                    schedulerClaimGate.acquire();
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                    throw new IllegalStateException(
                                            "ordinary Claim handoff was interrupted", interrupted);
                                }
                            },
                            schedulerOwnerEpoch::get,
                            schedulerEpoch::get,
                            negativeMonotonicClock,
                            ignored -> {});
                    final long waitDeadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
                    while (!ordinaryLoop.isWaitingForQueueChange() && System.nanoTime() < waitDeadline) {
                        Thread.sleep(1);
                    }
                    assertTrue(ordinaryLoop.isWaitingForQueueChange());
                    assertThrows(
                            IllegalStateException.class,
                            () -> claimHost.revokeClaim(
                                    otherWorker,
                                    budget(),
                                    originalClaim,
                                    bytes(32, 0xa0),
                                    (kind, delta) -> {},
                                    (a, b, c) -> guard(),
                                    () -> 100));
                    final long firstBeforeRevoke = store.latestSequenceNumber();
                    assertThrows(
                            IllegalStateException.class,
                            () -> claimHost.revokeClaim(
                                    claimWorker,
                                    budget(),
                                    originalClaim,
                                    bytes(32, 0xa1),
                                    (kind, delta) -> {},
                                    (a, b, c) -> {
                                        throw new IllegalStateException("revoke write authority unavailable");
                                    },
                                    () -> 100));
                    assertEquals(firstBeforeRevoke, store.latestSequenceNumber());
                    claimHost.revokeClaim(
                            claimWorker,
                            budget(),
                            originalClaim,
                            bytes(32, 0xa2),
                            (kind, delta) -> {},
                            (a, b, c) -> guard(),
                            () -> 100);
                    assertNull(store.get(ColumnFamily.INFLIGHT, originalClaim.key()));
                    assertNull(store.get(ColumnFamily.META, originalClaim.chargeKey()));
                    assertEquals(sourceSequenceBeforeRevoke, store.shardMutationSequence());
                    assertEquals(sourceBeforeRevoke, store.appliedShardLogPosition());
                    assertEquals(otherBeforeRevoke, otherStore.latestSequenceNumber());
                    final long afterRevoke = store.latestSequenceNumber();
                    final long writesAfterRevoke = store.operationStatistics().nativeWriteCalls();
                    final long otherWritesAfterRevoke = otherStore.operationStatistics().nativeWriteCalls();
                    assertThrows(
                            IllegalStateException.class,
                            () -> claimHost.revokeClaim(
                                    claimWorker,
                                    budget(),
                                    originalClaim,
                                    bytes(32, 0xa3),
                                    (kind, delta) -> {},
                                    (a, b, c) -> guard(),
                                    () -> 100));
                    assertEquals(afterRevoke, store.latestSequenceNumber());
                    assertNull(schedulerClaims.poll(150, java.util.concurrent.TimeUnit.MILLISECONDS));
                    assertEquals(afterRevoke, store.latestSequenceNumber());
                    assertEquals(otherBeforeRevoke, otherStore.latestSequenceNumber());
                    assertEquals(writesAfterRevoke, store.operationStatistics().nativeWriteCalls());
                    assertEquals(otherWritesAfterRevoke, otherStore.operationStatistics().nativeWriteCalls());
                    schedulerEpoch.set(schedulerDeadline);
                    try {
                        claim = schedulerClaims.poll(5, java.util.concurrent.TimeUnit.SECONDS);
                        assertNotNull(claim, () -> "ordinary scheduler failure: " + ordinaryLoop.firstFailure());
                        assertEquals(selectedHead, claim.selected());
                        assertEquals(actualOwner, claim.owner());
                        assertNotEquals(Bytes.hex(originalClaim.claimId()), Bytes.hex(claim.claimId()));
                        assertTrue(store.latestSequenceNumber() > afterRevoke);
                        assertEquals(otherBeforeRevoke, otherStore.latestSequenceNumber());

                        // Keep both physical Targets and both of Target A's source Shards eligible
                        // while the Host loop is paused at each post-commit Claim handoff. With
                        // Q=Cmax, physical Targets alternate and Target A rotates its source Shards.
                        claimHost.revokeClaim(
                                otherWorker,
                                budget(),
                                otherClaim,
                                bytes(32, 0xa5),
                                (kind, delta) -> {},
                                (a, b, c) -> guard(),
                                () -> 100);
                        claimHost.revokeClaim(
                                claimWorker,
                                budget(),
                                claim,
                                bytes(32, 0xa6),
                                (kind, delta) -> {},
                                (a, b, c) -> guard(),
                                () -> 100);
                        long firstBeforeNextClaim = store.latestSequenceNumber();
                        long otherBeforeNextClaim = otherStore.latestSequenceNumber();
                        schedulerEpoch.set(otherTargetDeliverAt);
                        schedulerClaimGate.release();
                        for (int turn = 1; turn < 5; turn++) {
                            final var nextClaim = schedulerClaims.poll(5, java.util.concurrent.TimeUnit.SECONDS);
                            assertNotNull(
                                    nextClaim,
                                    () -> "ordinary scheduler failure: " + ordinaryLoop.firstFailure());
                            final boolean targetBClaim = turn % 2 == 1;
                            assertEquals(
                                    targetBClaim ? otherPhysical.id() : physical.id(),
                                    nextClaim.selected().target());
                            final boolean firstClaimWorker = turn == 4;
                            assertEquals(firstClaimWorker ? actualOwner : otherOwner, nextClaim.owner());
                            if (firstClaimWorker) {
                                assertTrue(store.latestSequenceNumber() > firstBeforeNextClaim);
                                assertEquals(otherBeforeNextClaim, otherStore.latestSequenceNumber());
                            } else {
                                assertEquals(firstBeforeNextClaim, store.latestSequenceNumber());
                                assertTrue(otherStore.latestSequenceNumber() > otherBeforeNextClaim);
                            }
                            claim = nextClaim;
                            if (turn < 4) {
                                final var selectedWorker = firstClaimWorker ? claimWorker : otherWorker;
                                claimHost.revokeClaim(
                                        selectedWorker,
                                        budget(),
                                        nextClaim,
                                        bytes(32, 0xb0 + turn),
                                        (kind, delta) -> {},
                                        (a, b, c) -> guard(),
                                        () -> 100);
                                firstBeforeNextClaim = store.latestSequenceNumber();
                                otherBeforeNextClaim = otherStore.latestSequenceNumber();
                                schedulerClaimGate.release();
                            } else {
                                schedulerEpoch.set(0);
                                schedulerClaimGate.release();
                            }
                        }
                        final long schedulerIdleDeadline =
                                System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
                        while (!ordinaryLoop.isWaitingForQueueChange()
                                && System.nanoTime() < schedulerIdleDeadline) {
                            Thread.sleep(1);
                        }
                        assertTrue(ordinaryLoop.isWaitingForQueueChange());

                        final var replacementOwnerClock = new java.util.concurrent.atomic.AtomicLong(100);
                        final var replacementProbeInventory = claimHost.rebuildTargetInventory(
                                ordinaryInventoryLimits, replacementOwnerClock::get, System::nanoTime);
                        assertEquals(TargetWorkerTargetInventory.Stop.COMPLETE, replacementProbeInventory.stop());
                        final var replacementTargetBEntry = replacementProbeInventory.snapshot().targets().stream()
                                .filter(target -> target.id().equals(otherPhysical.id()))
                                .flatMap(target -> target.sources().stream())
                                .filter(source -> source.shard().equals(otherShard))
                                .map(TargetWorkerTargetInventory.Source::entry)
                                .findFirst()
                                .orElseThrow();
                        final var replacementTargetBHead = replacementTargetBEntry.queue().domains().stream()
                                .map(domain -> domain.ordinaryHead())
                                .filter(head -> head != null)
                                .findFirst()
                                .orElseThrow();
                        final long replacementTargetBCost = otherWorker
                                .probeSelectedHead(budget(), replacementTargetBHead, replacementOwnerClock::get)
                                .schedulingCost();
                        assertTrue(replacementTargetBCost > 1);
                        long replacementMaximumCost = 0;
                        for (final var target : replacementProbeInventory.snapshot().targets()) {
                            for (final var source : target.sources()) {
                                final var sourceWorker = source.shard().equals(scope.shard())
                                        ? claimWorker
                                        : otherWorker;
                                for (final var domain : source.entry().queue().domains()) {
                                    final var head = domain.ordinaryHead();
                                    if (head != null) {
                                        replacementMaximumCost = Math.max(
                                                replacementMaximumCost,
                                                sourceWorker.probeSelectedHead(
                                                                budget(), head, replacementOwnerClock::get)
                                                        .schedulingCost());
                                    }
                                }
                            }
                        }
                        assertTrue(replacementMaximumCost >= replacementTargetBCost);
                        final int replacementTargetCount = replacementProbeInventory.snapshot().targets().size();
                        final long replacementQuantum =
                                replacementTargetBCost / 2 + replacementTargetBCost % 2;
                        final var replacementDrr = claimHost.newOrdinaryDrr(
                                replacementProbeInventory,
                                new TargetWorkerOrdinaryDrr.Limits(
                                        replacementQuantum,
                                        replacementMaximumCost,
                                replacementMaximumCost,
                                replacementTargetCount,
                                4096,
                                32L << 20,
                                60_000_000_000L),
                                replacementOwnerClock::get,
                                System::nanoTime);
                        final long replacementNow = replacementTargetBHead.timeEpochMs();
                        final var replacementVisitBudget = new SchedulerBudget(
                                replacementTargetCount,
                                Math.multiplyExact(replacementMaximumCost, replacementTargetCount),
                                60_000_000_000L);
                        final OwnerIdentity[] replacementOwnerIdentity = {otherOwner};
                        final TargetWorkerOrdinaryDrr.Requests replacementRequests = (sourceWorker, cost) -> {
                            if (!cost.head().target().equals(otherPhysical.id())
                                    || !sourceWorker.shardId().equals(otherShard)) {
                                return java.util.Optional.empty();
                            }
                            final OwnerIdentity owner = sourceWorker.shardId().equals(otherShard)
                                    ? replacementOwnerIdentity[0]
                                    : actualOwner;
                            return java.util.Optional.of(new TargetWorkerOrdinaryDrr.Request(
                                    owner,
                                    Math.addExact(Math.max(replacementNow, cost.head().timeEpochMs()), 1000),
                                    bytes(32, 0x95),
                                    (kind, delta) -> {},
                                    (a, b, c) -> {
                                        throw new IllegalStateException("owner replacement credit reached commit");
                                    }));
                        };
                        assertEquals(
                                TargetWorkerOrdinaryDrr.FreezeStop.READY,
                                replacementDrr
                                        .freezeRecoveryFirstPass(
                                                replacementNow, replacementVisitBudget, replacementRequests)
                                        .stop());
                        final var beforeOwnerReplacement = replacementDrr.claimOrdinary(
                                replacementNow, replacementVisitBudget, replacementRequests);
                        assertTrue(beforeOwnerReplacement.claims().isEmpty());
                        assertEquals(TargetWorkerOrdinaryDrr.Stop.CREDIT_WAIT, beforeOwnerReplacement.stop());

                        final var oldStoreIncarnation = replacementProbeInventory
                                .snapshot()
                                .cuts()
                                .get(otherShard)
                                .storeIncarnation();
                        final var oldTargetBSourceIds = replacementProbeInventory.snapshot().targets().stream()
                                .filter(target -> target.id().equals(otherPhysical.id()))
                                .flatMap(target -> target.sources().stream())
                                .map(TargetWorkerTargetInventory.Source::shard)
                                .toList();
                        assertTrue(oldTargetBSourceIds.contains(otherShard));
                        com.nereusstream.delay.ownership.TargetWorkerHostRuntime.ShardDrain oldWorkerDrain;
                        final long drainDeadline = System.nanoTime()
                                + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
                        while (true) {
                            assertTrue(claimHost.awaitShardAdmission(otherShard, java.time.Duration.ofSeconds(1)),
                                    "old Worker head read did not release its Host admission");
                            try {
                                oldWorkerDrain = claimHost.drainShard(
                                        otherWorker,
                                        new TargetOwnerDrainCoordinator.Request(
                                                5_000,
                                                new SchedulerBudget(16, 32L << 20, 60_000_000_000L)),
                                        new SchedulerBudget(16, 32L << 20, 60_000_000_000L),
                                        () -> 100);
                                break;
                            } catch (IllegalStateException busy) {
                                if (!"Target host shard drain is already in progress".equals(busy.getMessage())
                                        || System.nanoTime() >= drainDeadline) {
                                    throw busy;
                                }
                            }
                        }
                        assertTrue(oldWorkerDrain.complete());
                        final var replacementActive = leases
                                .transition(
                                        leases.acquire(
                                                        otherAssignment,
                                                        "other-claim-worker-replacement",
                                                        ownerSession(0x85),
                                                        101,
                                                        10_000)
                                                .orElseThrow(),
                                        ShardLifecycleState.ACTIVE_FOR_COMMANDS)
                                .orElseThrow();
                        assertEquals(otherActive.ownerEpoch() + 1, replacementActive.ownerEpoch());
                        try (var replacementStore = ShardStore.openTarget(config, otherShard, resources)) {
                            replacementOwnerClock.set(101);
                            replacementStore.recordOpenedOwnerEpoch(replacementActive.ownerEpoch());
                            final TargetStoreBackend.ReadAuthority replacementOwnerReads =
                                    (actual, actualScope) -> new TargetStoreBackend.CommitGuard() {
                                        @Override
                                        public void requireCurrent() {
                                            assertEquals(otherScope, actualScope);
                                            assertArrayEquals(replacementStore.metadata().encode(), actual.encode());
                                            final var current = leases.current(otherShard).orElseThrow();
                                            assertTrue(replacementActive.sameIdentity(current));
                                            assertEquals(ShardLifecycleState.ACTIVE_FOR_COMMANDS, current.state());
                                            assertEquals(
                                                    replacementActive.ownerEpoch(),
                                                    replacementStore.runtimeMetadata().lastOpenedOwnerEpoch());
                                        }

                                        @Override
                                        public void close() {}
                                    };
                            final var outcomeAuthorityResolutions = new java.util.concurrent.atomic.AtomicInteger();
                            final var expectedPublishedEvidence = new java.util.concurrent.atomic.AtomicReference<
                                    com.nereusstream.delay.protocol.PublishEvidence>();
                            final var retryAdmissionImage =
                                    new java.util.concurrent.atomic.AtomicReference<SystemMutation>();
                            final var firstRetryAdmissionImage =
                                    new java.util.concurrent.atomic.AtomicReference<SystemMutation>();
                            final var replacementInitialized = TargetStoreBootstrap.reopen(
                                    replacementStore,
                                    otherScope,
                                    new TargetStoreBackend.WriteLimits(64, 2 << 20),
                                    budget(),
                                    replacementOwnerReads);
                            final var expectedLateCommand = new java.util.concurrent.atomic.AtomicReference<
                                    PreparedCommand>();
                            final var replacementRuntime = new TargetSourceApplyRuntime(
                                    replacementInitialized,
                                    replacementStore,
                                    otherAssignment,
                                    replacementActive,
                                    new TargetSourceApplyRuntime.Authorities(
                                            leases,
                                            SourceReplaySuccessor.strictKafka(),
                                            entry -> {
                                                throw new AssertionError("replacement Shard resolved a grant");
                                            },
                                            entry -> {
                                                throw new AssertionError("replacement Shard resolved a fence");
                                            },
                                            entry -> {
                                                throw new AssertionError("unexpected Target expiry authority");
                                            },
                                            entry -> {
                                                throw new AssertionError("replacement Shard resolved a Close");
                                            },
                                            entry -> {
                                                throw new AssertionError("replacement Shard resolved membership");
                                            },
                                            (a, b, c) -> guard(),
                                            replacementOwnerReads,
                                            entry -> {
                                                final var expected = expectedLateCommand.get();
                                                if (expected == null || !expected.equals(entry.command())) {
                                                    throw new AssertionError(
                                                            "replacement Shard resolved an unexpected command");
                                                }
                                                return new TargetSourceApplyRuntime.CommandControl(
                                                        otherPolicy,
                                                        (reader, bound, source) -> false,
                                                        (reader, bound) -> java.util.Optional.empty(),
                                                        (incoming, source) -> {
                                                            if (incoming.type() != CommandType.SCHEDULE
                                                                    || !incoming.equals(expected)) {
                                                                throw new AssertionError(
                                                                        "replacement Shard resolved another Schedule");
                                                            }
                                                            final var acceptedBinding = new TargetScheduleBinding(
                                                                    incoming.delayMessageId(),
                                                                    incoming.type(),
                                                                    incoming.canonicalBody(),
                                                                    source,
                                                                    physical.id(),
                                                                    initial.domain(),
                                                                    otherActivation
                                                                            .allocation()
                                                                            .identity()
                                                                            .accountingIncarnation(),
                                                                    dispatch.digest(),
                                                                    dispatch.digest(),
                                                                    otherControls.digest(),
                                                                    otherMembership.digest(),
                                                                    null,
                                                                    otherOrderingDomain);
                                                            return new TargetCommandStore.ScheduleAdmission(
                                                                    StableCode.OK,
                                                                    new TargetScheduleRegistration.Authority(
                                                                            acceptedBinding,
                                                                            physical,
                                                                            destination,
                                                                            capability,
                                                                            otherProfiles,
                                                                            60000),
                                                                    TargetOrderState.OrderingContract
                                                                            .ADMISSION_WATERMARK);
                                                        },
                                                        noProofs(),
                                                        (a, b, c) -> guard());
                                            },
                                            entry -> {
                                                throw new AssertionError(
                                                        "replacement Shard resolved a Native policy control");
                                            },
                                            entry -> new TargetSourceApplyRuntime.AdmissionControl(
                                                    (actualScope, writer, mutation, source) -> {
                                                        retryAdmissionImage.set(mutation);
                                                        if (TargetPublishAdmissionBody.decode(mutation.canonicalBody())
                                                                        .attemptNo()
                                                                == 1) {
                                                            firstRetryAdmissionImage.set(mutation);
                                                        }
                                                        assertEquals(otherScope, actualScope);
                                                        final var writerOwner = replacementOwnerIdentity[0];
                                                        assertArrayEquals(
                                                                AuthorIdentity.owner(
                                                                                writerOwner.deploymentId(),
                                                                                writerOwner.workerRunId(),
                                                                                writerOwner.ownerEpoch(),
                                                                                writerOwner.leaseFencingDigest())
                                                                        .canonicalBytes(),
                                                                writer.canonicalBytes());
                                                        return new TargetPublishAdmissionVerifier.Authorization(
                                                                keys.getPublic(),
                                                                TargetPublishAdmissionBody.decode(
                                                                                mutation.canonicalBody())
                                                                                .publication() == null
                                                                        ? ProtocolTuple.targetPublishAdmission()
                                                                        : ProtocolTuple
                                                                                .targetMaterializedPublishAdmission(),
                                                                10,
                                                                10,
                                                                100,
                                                                (boundScope, boundWriter, position, evidence) -> true,
                                                                (body, position) -> {});
                                                    },
                                                    (a, b, c) -> guard()),
                                            entry -> new TargetSourceApplyRuntime.OutcomeControl(
                                                    (actualScope, writer, mutation, source) -> {
                                                        outcomeAuthorityResolutions.incrementAndGet();
                                                        assertEquals(otherScope, actualScope);
                                                        assertEquals(
                                                                SystemMutationType.PUBLISH_OUTCOME,
                                                                mutation.type());
                                                        final var writerOwner = replacementOwnerIdentity[0];
                                                        assertArrayEquals(
                                                                AuthorIdentity.owner(
                                                                                writerOwner.deploymentId(),
                                                                                writerOwner.workerRunId(),
                                                                                writerOwner.ownerEpoch(),
                                                                                writerOwner.leaseFencingDigest())
                                                                        .canonicalBytes(),
                                                                writer.canonicalBytes());
                                                        return new TargetPublishOutcomeVerifier.Authorization(
                                                                keys.getPublic(),
                                                                ProtocolTuple.currentSystemMutation(),
                                                                writerOwner,
                                                                10,
                                                                10,
                                                                100,
                                                                (boundScope, boundWriter, position, evidence) -> true,
                                                                new TargetPublishOutcomeVerifier.RetryContext(
                                                                        retryAdmissionImage.get(),
                                                                        firstRetryAdmissionImage.get(),
                                                                        otherRetryPolicy),
                                                                publishedOutcome
                                                                        ? publishedEvidenceContext(
                                                                                retryAdmissionImage.get(),
                                                                                expectedPublishedEvidence.get())
                                                                        : null);
                                                    },
                                                    (a, b, c) -> guard())),
                                    new TargetSourceApplyRuntime.Limits(
                                            4096, 32L << 20, 60_000_000_000L, 16, 1),
                                    System::nanoTime);
                            final var replacementNextSourceRecord = new java.util.concurrent.atomic.AtomicReference<
                                    SourceRecordConsumer.PolledSourceRecord>();
                            final var replacementWorker = TargetWorkerShardFactory.create(
                                    () -> java.util.Optional.ofNullable(
                                            replacementNextSourceRecord.getAndSet(null)),
                                    replacementRuntime.acceptedAssignment(),
                                    workerClasses,
                                    replacementStore,
                                    resources,
                                    replacementRuntime,
                                    new TargetWorkerShardRuntime.Maintenance(
                                            new TargetCloseStore(
                                                            replacementInitialized.backend(),
                                                            otherScope,
                                                            replacementInitialized.root().recoveryLineage(),
                                                            16,
                                                            1)
                                                    .reservationControls(
                                                            (reader, bound) -> java.util.Optional.empty()),
                                            new TargetReservationClosureWorkClassExecutor.Limits(
                                                    4096, 250_000, 60_000_000_000L),
                                            new TargetReservationExpiryWorkClassExecutor.Limits(
                                                    2048, 100_000, 60_000_000_000L),
                                            (a, b, c) -> guard(),
                                            ignored -> {},
                                            ignored -> {},
                                            ignored -> {},
                                            () -> 101));
                            replacementOwnerIdentity[0] = new OwnerIdentity(
                                    bytes(16, 0x92),
                                    bytes(16, 0x93),
                                    replacementActive.ownerEpoch(),
                                    replacementActive.leaseToken());
                            replacementLoopShard.set(replacementWorker);
                            replacementLoopOwner.set(replacementOwnerIdentity[0]);
                            replacementLoopOnly.set(true);
                            schedulerOwnerEpoch.set(101);
                            schedulerEpoch.set(replacementNow);
                            final long sourceSequenceBeforeDynamicClaim = store.latestSequenceNumber();
                            final long replacementSequenceBeforeDynamicClaim = replacementStore.latestSequenceNumber();
                            claimHost.admitShard(replacementWorker);
                            final var replacementLoopClaim = schedulerClaims.poll(
                                    5, java.util.concurrent.TimeUnit.SECONDS);
                            assertNotNull(
                                    replacementLoopClaim,
                                    () -> "ordinary scheduler failure after Worker admission: "
                                            + ordinaryLoop.firstFailure());
                            assertEquals(physical.id(), replacementLoopClaim.selected().target());
                            assertEquals(replacementOwnerIdentity[0], replacementLoopClaim.owner());
                            assertEquals(sourceSequenceBeforeDynamicClaim, store.latestSequenceNumber());
                            assertTrue(replacementStore.latestSequenceNumber() > replacementSequenceBeforeDynamicClaim);
                            final var replacementCacheBudget = budget();
                            assertTrue(replacementWorker
                                    .readTargetQueue(
                                            replacementCacheBudget, otherPhysical.id(), replacementOwnerClock::get)
                                    .isPresent());
                            assertEquals(0, replacementCacheBudget.actualRecords());
                            final var replacementInventory = claimHost.rebuildTargetInventory(
                                    ordinaryInventoryLimits, () -> 101, System::nanoTime);
                            assertEquals(TargetWorkerTargetInventory.Stop.COMPLETE, replacementInventory.stop());
                            assertArrayEquals(
                                    oldStoreIncarnation,
                                    replacementInventory.snapshot().cuts().get(otherShard).storeIncarnation());
                            assertEquals(
                                    oldTargetBSourceIds,
                                    replacementInventory.snapshot().targets().stream()
                                            .filter(target -> target.id().equals(otherPhysical.id()))
                                            .flatMap(target -> target.sources().stream())
                                            .map(TargetWorkerTargetInventory.Source::shard)
                                            .toList());
                            replacementDrr.refreshInventory(replacementInventory);
                            final long sequenceBeforeCreditCheck = replacementStore.latestSequenceNumber();
                            final var afterOwnerReplacement = replacementDrr.claimOrdinary(
                                    replacementNow, replacementVisitBudget, replacementRequests);
                            assertTrue(afterOwnerReplacement.claims().isEmpty());
                            assertEquals(TargetWorkerOrdinaryDrr.Stop.CREDIT_WAIT, afterOwnerReplacement.stop());
                            assertEquals(sequenceBeforeCreditCheck, replacementStore.latestSequenceNumber());
                            final var creditReachedCommitBarrier = assertThrows(
                                    IllegalStateException.class,
                                    () -> replacementDrr.claimOrdinary(
                                            replacementNow, replacementVisitBudget, replacementRequests));
                            assertEquals(
                                    "owner replacement credit reached commit", creditReachedCommitBarrier.getMessage());
                            assertEquals(sequenceBeforeCreditCheck, replacementStore.latestSequenceNumber());
                            if (!uncertainRetry) {
                                schedulerEpoch.set(0);
                                schedulerClaimGate.release();
                                ordinaryLoop.close();
                            }

                            final var materializedChannel = closedRetryQueue || publishedOutcome
                                    ? materializedChannelFixture(
                                            replacementInitialized.backend(), replacementStore, otherScope,
                                            replacementInitialized.root().recoveryLineage(), replacementLoopClaim,
                                            otherBinding, destination.ref())
                                    : null;
                            final var admissionBefore =
                                    (KafkaSourcePosition) replacementStore.appliedShardLogPosition();
                            final var admissionAt = source(
                                    admissionBefore,
                                    admissionBefore.offset() + 1,
                                    uncertainRetry
                                            ? Math.max(admissionBefore.brokerLogAppendTimeEpochMs() + 1,
                                                    schedulerEpoch.get())
                                            : admissionBefore.brokerLogAppendTimeEpochMs() + 1);
                            final var admission = targetAdmission(
                                    replacementStore,
                                    replacementLoopClaim,
                                    replacementOwnerIdentity[0],
                                    keys,
                                    admissionAt,
                                    materializedChannel,
                                    capability.ref());
                            if (closedRetryQueue || publishedOutcome) {
                                final var materialized = TargetPublishAdmissionBody.decode(
                                        admission.entry().mutation().canonicalBody());
                                assertEquals(4, materialized.bodyVersion());
                                assertEquals(materializedChannel, materialized.publication().channel());
                            }
                            final var admissionAcknowledgements = new java.util.concurrent.atomic.AtomicInteger();
                            replacementNextSourceRecord.set(new SourceRecordConsumer.PolledSourceRecord(
                                    admission.entry(),
                                    (entry, outcome) -> {
                                        assertEquals(admission.entry(), entry);
                                        assertEquals(admissionAt, outcome.position());
                                        assertEquals(StableCode.OK, outcome.systemMutationResult().stableCode());
                                        assertEquals(ApplyStatus.APPLIED, outcome.systemMutationResult().applyStatus());
                                        return admissionAcknowledgements.incrementAndGet() == 1
                                                ? SourceAcknowledgement.AcknowledgementResult.unknown(null)
                                                : SourceAcknowledgement.AcknowledgementResult.acked();
                                    }));
                            final long beforeAdmissionMutation = replacementStore.shardMutationSequence();
                            final long beforeAdmissionVersion = replacementStore.latestSequenceNumber();
                            final var admissionUnknownAck = replacementWorker.runSourceTurn(
                                    new SchedulerBudget(64, 32L << 20, 60_000_000_000L), () -> 101);
                            assertEquals(
                                    SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN,
                                    admissionUnknownAck.status(),
                                    () -> String.valueOf(admissionUnknownAck.failure()));
                            assertEquals(admission.entry(), replacementWorker.pendingSourceEntry().orElseThrow());
                            assertEquals(1, admissionAcknowledgements.get());
                            assertEquals(beforeAdmissionMutation + 1, replacementStore.shardMutationSequence());
                            assertTrue(replacementStore.latestSequenceNumber() > beforeAdmissionVersion);
                            assertEquals(admissionAt, replacementStore.appliedShardLogPosition());
                            assertTargetAdmissionCommitted(replacementStore, replacementLoopClaim, admission);

                            final long versionAfterAdmission = replacementStore.latestSequenceNumber();
                            final var admissionAcked = replacementWorker.runSourceTurn(
                                    new SchedulerBudget(64, 32L << 20, 60_000_000_000L), () -> 101);
                            assertEquals(SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED, admissionAcked.status());
                            assertEquals(admission.entry(), admissionAcked.entry());
                            assertEquals(2, admissionAcknowledgements.get());
                            assertTrue(replacementWorker.pendingSourceEntry().isEmpty());
                            assertEquals(beforeAdmissionMutation + 1, replacementStore.shardMutationSequence());
                            assertEquals(versionAfterAdmission, replacementStore.latestSequenceNumber());
                            assertEquals(admissionAt, replacementStore.appliedShardLogPosition());

                            final byte[] admittedOrderKey = uncertainRetry
                                    ? null
                                    : TargetKeyCodec.orderState(
                                            replacementLoopClaim.work().locator().target(),
                                            replacementLoopClaim.work().locator().orderingDomain());
                            TargetOrderState orderAfterCancel = null;
                            if (!uncertainRetry) {
                                final byte[] admittedOrderState =
                                        replacementStore.get(ColumnFamily.META, admittedOrderKey);
                                final var admittedOrder = TargetOrderState.decode(TargetValueEnvelope.decode(
                                                admittedOrderState, TargetOrderState.VALUE_TYPE)
                                        .payload());
                                final long lateDeliverAt = admittedOrder.lastAdmittedOrder().deliverAtEpochMs() - 1;
                                final var lateAt = source(
                                        admissionAt,
                                        admissionAt.offset() + 1,
                                        admissionAt.brokerLogAppendTimeEpochMs() + 1);
                                final var lateIntent = CanonicalScheduleIntent.create(
                                        destination.ref(),
                                        otherIntent.retryPolicy(),
                                        lateDeliverAt,
                                        lateAt.brokerLogAppendTimeEpochMs() + 2000,
                                        otherIntent.deliveryMode(),
                                        OrderingMode.DELIVERY_TIME_FIFO,
                                        otherIntent.orderingKey(),
                                        model.inlinePayload(),
                                        null,
                                        otherIntent.adapterMetadata(),
                                        otherIntent.businessKey(),
                                        otherIntent.eventTimeEpochMs(),
                                        NativeDeliveryPolicy.FORBID);
                                final var lateSchedule = schedule(
                                        lateIntent, otherSchedule.delayMessageId(), lateAt, 44);
                                expectedLateCommand.set(lateSchedule);
                                final var lateAcknowledgements = new java.util.concurrent.atomic.AtomicInteger();
                                final var lateEntry = new SourceReplayRecord(lateSchedule, lateAt, null, null);
                                replacementNextSourceRecord.set(new SourceRecordConsumer.PolledSourceRecord(
                                        lateEntry,
                                        (entry, outcome) -> {
                                            assertEquals(lateEntry, entry);
                                            assertEquals(lateAt, outcome.position());
                                            assertEquals(
                                                    StableCode.ORDER_BEFORE_ADMISSION_WATERMARK,
                                                    outcome.commandResult().stableCode());
                                            assertEquals(1, lateAcknowledgements.incrementAndGet());
                                            return SourceAcknowledgement.AcknowledgementResult.acked();
                                        }));
                                final long beforeLateScheduleMutation = replacementStore.shardMutationSequence();
                                final long beforeLateScheduleVersion = replacementStore.latestSequenceNumber();
                                final var lateScheduleTurn = replacementWorker.runSourceTurn(
                                        new SchedulerBudget(64, 32L << 20, 60_000_000_000L), () -> 101);
                                assertEquals(
                                        SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED,
                                        lateScheduleTurn.status(),
                                        () -> String.valueOf(lateScheduleTurn.failure()));
                                assertEquals(lateEntry, lateScheduleTurn.entry());
                                assertEquals(1, lateAcknowledgements.get());
                                assertTrue(replacementWorker.pendingSourceEntry().isEmpty());
                                assertEquals(
                                        beforeLateScheduleMutation + 1,
                                        replacementStore.shardMutationSequence());
                                assertTrue(replacementStore.latestSequenceNumber() > beforeLateScheduleVersion);
                                assertEquals(lateAt, replacementStore.appliedShardLogPosition());
                                assertNull(replacementStore.get(
                                        ColumnFamily.ID, TargetKeyCodec.message(lateSchedule.delayMessageId())));
                                assertArrayEquals(
                                        admittedOrderState, replacementStore.get(ColumnFamily.META, admittedOrderKey));

                                final var lateRescheduleBefore =
                                        (KafkaSourcePosition) replacementStore.appliedShardLogPosition();
                                final var lateRescheduleAt = source(
                                        lateRescheduleBefore,
                                        lateRescheduleBefore.offset() + 1,
                                        lateRescheduleBefore.brokerLogAppendTimeEpochMs() + 1);
                                final var lateReschedule = PreparedCommand.reschedule(
                                        otherShard,
                                        cancel(otherFollowerSchedule.delayMessageId(), lateRescheduleAt, 46)
                                                .commandId(),
                                        otherFollowerSchedule.delayMessageId(),
                                        new MessagePrecondition(0L, null),
                                        lateDeliverAt,
                                        lateRescheduleAt.brokerLogAppendTimeEpochMs() + 2_000,
                                        lateRescheduleAt.brokerLogAppendTimeEpochMs() + 1_000);
                                expectedLateCommand.set(lateReschedule);
                                final var lateRescheduleEntry =
                                        new SourceReplayRecord(lateReschedule, lateRescheduleAt, null, null);
                                final byte[] followerMessageKey =
                                        TargetKeyCodec.message(otherFollowerSchedule.delayMessageId());
                                final byte[] followerMessageBefore =
                                        replacementStore.get(ColumnFamily.ID, followerMessageKey);
                                assertNotNull(followerMessageBefore);
                                final var lateRescheduleAcknowledgements =
                                        new java.util.concurrent.atomic.AtomicInteger();
                                replacementNextSourceRecord.set(new SourceRecordConsumer.PolledSourceRecord(
                                        lateRescheduleEntry,
                                        (entry, outcome) -> {
                                            assertEquals(lateRescheduleEntry, entry);
                                            assertEquals(lateRescheduleAt, outcome.position());
                                            assertEquals(ApplyStatus.REJECTED, outcome.commandResult().applyStatus());
                                            assertEquals(
                                                    StableCode.ORDER_BEFORE_ADMISSION_WATERMARK,
                                                    outcome.commandResult().stableCode());
                                            assertEquals(1, lateRescheduleAcknowledgements.incrementAndGet());
                                            return SourceAcknowledgement.AcknowledgementResult.acked();
                                        }));
                                final long beforeLateRescheduleMutation = replacementStore.shardMutationSequence();
                                final long beforeLateRescheduleVersion = replacementStore.latestSequenceNumber();
                                final var lateRescheduleTurn = replacementWorker.runSourceTurn(
                                        new SchedulerBudget(64, 32L << 20, 60_000_000_000L), () -> 101);
                                assertEquals(
                                        SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED,
                                        lateRescheduleTurn.status(),
                                        () -> String.valueOf(lateRescheduleTurn.failure()));
                                assertEquals(lateRescheduleEntry, lateRescheduleTurn.entry());
                                assertEquals(1, lateRescheduleAcknowledgements.get());
                                assertTrue(replacementWorker.pendingSourceEntry().isEmpty());
                                assertEquals(
                                        beforeLateRescheduleMutation + 1,
                                        replacementStore.shardMutationSequence());
                                assertTrue(replacementStore.latestSequenceNumber() > beforeLateRescheduleVersion);
                                assertEquals(lateRescheduleAt, replacementStore.appliedShardLogPosition());
                                assertArrayEquals(
                                        followerMessageBefore,
                                        replacementStore.get(ColumnFamily.ID, followerMessageKey));
                                assertArrayEquals(
                                        admittedOrderState, replacementStore.get(ColumnFamily.META, admittedOrderKey));

                                final var lateCancelBefore =
                                        (KafkaSourcePosition) replacementStore.appliedShardLogPosition();
                                final var lateCancelAt = source(
                                        lateCancelBefore,
                                        lateCancelBefore.offset() + 1,
                                        lateCancelBefore.brokerLogAppendTimeEpochMs() + 1);
                                final var lateCancel = cancel(
                                        otherFollowerSchedule.delayMessageId(), lateCancelAt, 47);
                                expectedLateCommand.set(lateCancel);
                                final var lateCancelEntry =
                                        new SourceReplayRecord(lateCancel, lateCancelAt, null, null);
                                final var lateCancelAcknowledgements = new java.util.concurrent.atomic.AtomicInteger();
                                replacementNextSourceRecord.set(new SourceRecordConsumer.PolledSourceRecord(
                                        lateCancelEntry,
                                        (entry, outcome) -> {
                                            assertEquals(lateCancelEntry, entry);
                                            assertEquals(lateCancelAt, outcome.position());
                                            assertEquals(StableCode.CANCELED, outcome.commandResult().stableCode());
                                            assertEquals(1, lateCancelAcknowledgements.incrementAndGet());
                                            return SourceAcknowledgement.AcknowledgementResult.acked();
                                        }));
                                final long beforeLateCancelMutation = replacementStore.shardMutationSequence();
                                final long beforeLateCancelVersion = replacementStore.latestSequenceNumber();
                                final var lateCancelTurn = replacementWorker.runSourceTurn(
                                        new SchedulerBudget(64, 32L << 20, 60_000_000_000L), () -> 101);
                                assertEquals(
                                        SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED,
                                        lateCancelTurn.status(),
                                        () -> String.valueOf(lateCancelTurn.failure()));
                                assertEquals(lateCancelEntry, lateCancelTurn.entry());
                                assertEquals(1, lateCancelAcknowledgements.get());
                                assertTrue(replacementWorker.pendingSourceEntry().isEmpty());
                                assertEquals(beforeLateCancelMutation + 1, replacementStore.shardMutationSequence());
                                assertTrue(replacementStore.latestSequenceNumber() > beforeLateCancelVersion);
                                assertEquals(lateCancelAt, replacementStore.appliedShardLogPosition());
                                final var canceledFollower = TargetMessageRecord.decodeForStore(
                                        followerMessageKey,
                                        TargetValueEnvelope.decode(
                                                        replacementStore.get(ColumnFamily.ID, followerMessageKey),
                                                        TargetMessageRecord.VALUE_TYPE)
                                                .payload(),
                                        otherShard);
                                assertEquals(GenerationAggregateState.CANCELED, canceledFollower.aggregateState());
                                assertEquals(CurrentSendWorkKind.NONE, canceledFollower.runtime().currentWorkKind());
                                final var canceledTerminal = TargetTerminalGenerationRecord.decode(
                                        TargetValueEnvelope.decode(
                                                        replacementStore.get(
                                                                ColumnFamily.TERMINAL,
                                                                TargetTerminalGenerationRecord.key(
                                                                        canceledFollower.locator())),
                                                        TargetTerminalGenerationRecord.VALUE_TYPE)
                                                .payload());
                                assertEquals(canceledFollower.locator(), canceledTerminal.locator());
                                assertEquals(StableCode.CANCELED, canceledTerminal.terminalCode());
                                orderAfterCancel = TargetOrderState.decode(TargetValueEnvelope.decode(
                                                replacementStore.get(ColumnFamily.META, admittedOrderKey),
                                                TargetOrderState.VALUE_TYPE)
                                        .payload());
                                orderAfterCancel.requireSuccessorOf(admittedOrder);
                                assertArrayEquals(
                                        admittedOrder.lastAdmittedOrder().encodedKey(),
                                        orderAfterCancel.lastAdmittedOrder().encodedKey());
                                assertArrayEquals(
                                        admittedOrder.barrier().canonicalBytes(),
                                        orderAfterCancel.barrier().canonicalBytes());
                                assertNull(orderAfterCancel.serviceableHead());
                            }
                            if (publishFailure == 10 || publishFailure == 11) {
                                assertRecoveredTargetOutcome(replacementStore, otherScope, replacementWorker,
                                        replacementActive, otherAssignment, leases,
                                        takeoverLeases == null ? leases : takeoverLeases,
                                        takeoverSession == null ? bytes(32, 0xF1) : takeoverSession,
                                        resources, workerClasses, admission, replacementOwnerIdentity[0], keys,
                                        otherRetryPolicy, publishFailure == 11);
                                claimWorker.pauseNewTurns();
                                claimWorker.closeSource();
                                return;
                            }
                            if (publishFailure >= 12) {
                                assertLateTargetOutcome(replacementInitialized.backend(), replacementStore, otherScope,
                                        replacementInitialized.root().recoveryLineage(), replacementWorker,
                                        replacementNextSourceRecord, admission, keys, replacementOwnerIdentity[0],
                                        expectedPublishedEvidence, outcomeAuthorityResolutions, publishFailure);
                                replacementWorker.pauseNewTurns();
                                replacementWorker.closeSource();
                                claimWorker.pauseNewTurns();
                                claimWorker.closeSource();
                                return;
                            }
                            if (publishedOutcome) {
                                assertPublishedTargetOutcome(
                                        replacementInitialized.backend(), replacementStore, otherScope,
                                        replacementInitialized.root().recoveryLineage(), replacementWorker,
                                        replacementNextSourceRecord, admission, keys, replacementOwnerIdentity[0],
                                        expectedPublishedEvidence, outcomeAuthorityResolutions, mismatchedTransfer,
                                        workerClasses, publishFailure);
                            } else {
                                if (closedRetryQueue) {
                                    closeRetryQueueFixture(
                                            replacementInitialized.backend(),
                                            replacementStore,
                                            otherScope,
                                            replacementInitialized.root().recoveryLineage(),
                                            replacementLoopClaim.work().locator().target());
                                }

                                final var admittedBody =
                                        TargetPublishAdmissionBody.decode(admission.entry().mutation().canonicalBody());
                                final var outcomeBefore =
                                        (KafkaSourcePosition) replacementStore.appliedShardLogPosition();
                                final var outcomeAt = source(
                                        outcomeBefore,
                                        outcomeBefore.offset() + 1,
                                        uncertainRetry
                                                ? Math.max(
                                                        Math.max(outcomeBefore.brokerLogAppendTimeEpochMs() + 1,
                                                                admittedBody.decisionTime().latestEpochMs() + 1),
                                                        schedulerEpoch.get() + 1)
                                                : outcomeBefore.brokerLogAppendTimeEpochMs() + 1);
                                final byte[] outcomeBudgetKey = Bytes.concat(
                                        new byte[] {
                                            (byte) TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG,
                                            TargetKeyCodec.KEY_FORMAT
                                        },
                                        admission.attemptId());
                                final var admittedBudget = TargetQuotaAttemptBudget.decode(TargetValueEnvelope.decode(
                                                replacementStore.get(ColumnFamily.META, outcomeBudgetKey),
                                                TargetQuotaAttemptBudget.VALUE_TYPE)
                                        .payload());
                                final long outcomeRetryUntil =
                                        outcomeAt.brokerLogAppendTimeEpochMs() + 10_000;
                                final var outcomeObservedAt = new TrustedUtcIntervalEvidence(
                                        outcomeAt.brokerLogAppendTimeEpochMs(),
                                        outcomeAt.brokerLogAppendTimeEpochMs() + 1,
                                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                                        bytes(32, 0xA1),
                                        1,
                                        1,
                                        1,
                                        bytes(32, 0xA2),
                                        0,
                                        new byte[0]);
                                final long firstAttemptAt = admittedBody.decisionTime().latestEpochMs();
                                final long retryDeadline = Math.min(
                                        otherIntent.expireAtEpochMs(),
                                        firstAttemptAt + otherRetryPolicy.maxRetryDurationMs());
                                final Long nextRetryAt = uncertainRetry
                                        ? outcomeObservedAt.latestEpochMs()
                                                + com.nereusstream.delay.protocol.RetryJitter.delayMs(
                                                        com.nereusstream.delay.protocol.RetryJitter.MESSAGE_PUBLISH,
                                                        admittedBody.locator().messageId(),
                                                        Integer.toUnsignedLong(admittedBody.locator().generation()),
                                                        Integer.toUnsignedLong(admittedBody.attemptNo()),
                                                        otherRetryPolicy.retryBackoffCap(admittedBody.attemptNo()))
                                        : null;
                                final byte[] unknownRetry = typedUnknownRetryDecision(
                                        otherRetryPolicy, firstAttemptAt, retryDeadline,
                                        admittedBody.attemptNo(), nextRetryAt);
                                final byte[] unknownTransfer = new PublishAdmissionBody.ChargeVector(
                                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
                                        .canonicalBytes();
                                final byte[] unknownOutcomeBody = PublishOutcomeBody.encodeInitial(
                                        otherShard,
                                        outcomeRetryUntil,
                                        admission.attemptId(),
                                        3,
                                        4,
                                        StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                                        null,
                                        unknownTransfer,
                                        outcomeObservedAt,
                                        unknownRetry);
                                final var outcomeOwner = replacementOwnerIdentity[0];
                                final var outcomeMutation = SystemMutation.signed(
                                        otherShard,
                                        SystemMutationType.PUBLISH_OUTCOME,
                                        outcomeRetryUntil,
                                        admission.attemptId(),
                                        unknownOutcomeBody,
                                        AuthorIdentity.owner(
                                                        outcomeOwner.deploymentId(),
                                                        outcomeOwner.workerRunId(),
                                                        outcomeOwner.ownerEpoch(),
                                                        outcomeOwner.leaseFencingDigest())
                                                .canonicalBytes(),
                                        1,
                                        keys.getPrivate());
                                if (!uncertainRetry) {
                                    assertRecoveryOutcomeSnapshot(replacementWorker, replacementStore,
                                            admission.entry().mutation(), outcomeOwner, keys, otherRetryPolicy,
                                            outcomeRetryUntil, outcomeObservedAt, unknownRetry, unknownTransfer,
                                            outcomeMutation);
                                }
                                final var outcomeEntry =
                                        new SourceReplayMutation(outcomeMutation, outcomeAt, null, null);
                                assertUnprovedTypedTargetOutcomeLeavesSourceUnchanged(
                                        replacementInitialized.backend(),
                                        replacementStore,
                                        otherScope,
                                        replacementInitialized.root().recoveryLineage(),
                                        outcomeEntry,
                                        outcomeOwner,
                                        admission.entry().mutation(),
                                        otherRetryPolicy,
                                        keys);
                                final var outcomeAcknowledgements = new java.util.concurrent.atomic.AtomicInteger();
                                replacementNextSourceRecord.set(new SourceRecordConsumer.PolledSourceRecord(
                                        outcomeEntry,
                                        (entry, outcome) -> {
                                            assertEquals(outcomeEntry, entry);
                                            assertEquals(outcomeAt, outcome.position());
                                            assertEquals(ApplyStatus.APPLIED,
                                                    outcome.systemMutationResult().applyStatus());
                                            assertEquals(
                                                    StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                                                    outcome.systemMutationResult().stableCode());
                                            return outcomeAcknowledgements.incrementAndGet() == 1
                                                    ? SourceAcknowledgement.AcknowledgementResult.unknown(null)
                                                    : SourceAcknowledgement.AcknowledgementResult.acked();
                                        }));
                                final long beforeOutcomeMutation = replacementStore.shardMutationSequence();
                                final long beforeOutcomeVersion = replacementStore.latestSequenceNumber();
                                final var unknownOutcomeTurn = replacementWorker.runSourceTurn(
                                        new SchedulerBudget(64, 32L << 20, 60_000_000_000L), () -> 101);
                                assertEquals(
                                        SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN,
                                        unknownOutcomeTurn.status(),
                                        () -> String.valueOf(unknownOutcomeTurn.failure()));
                                assertEquals(outcomeEntry, replacementWorker.pendingSourceEntry().orElseThrow());
                                assertEquals(1, outcomeAcknowledgements.get());
                                assertEquals(beforeOutcomeMutation + 1, replacementStore.shardMutationSequence());
                                assertTrue(replacementStore.latestSequenceNumber() > beforeOutcomeVersion);
                                assertEquals(outcomeAt, replacementStore.appliedShardLogPosition());
                                final byte[] admittedMessageKey =
                                        TargetKeyCodec.message(replacementLoopClaim.work().locator().messageId());
                                final var uncertainMessage = TargetMessageRecord.decodeForStore(
                                        admittedMessageKey,
                                        TargetValueEnvelope.decode(
                                                        replacementStore.get(ColumnFamily.ID, admittedMessageKey),
                                                        TargetMessageRecord.VALUE_TYPE)
                                                .payload(),
                                        otherShard);
                                assertEquals(GenerationAggregateState.UNCERTAIN, uncertainMessage.aggregateState());
                                assertEquals(
                                        uncertainRetry && !closedRetryQueue
                                                ? CurrentSendWorkKind.TIMELINE
                                                : CurrentSendWorkKind.NONE,
                                        uncertainMessage.runtime().currentWorkKind());
                                if (uncertainRetry && !closedRetryQueue) {
                                    final var retryWork = uncertainMessage.runtime().timeline();
                                    assertEquals(TimelineWorkKind.UNCERTAIN_RETRY, retryWork.workKind());
                                    assertEquals(UncertainRetryAuthority.PINNED_POLICY,
                                            retryWork.uncertainRetryAuthority());
                                    assertEquals(2, retryWork.candidateAttemptNo());
                                    assertFalse(retryWork.nativeCandidate());
                                    assertEquals(Math.max(uncertainMessage.deliverAtEpochMs(), nextRetryAt),
                                            retryWork.ordinaryEligibilityAtEpochMs());
                                    uncertainMessage.requireTimelineProjection(
                                            retryWork.ordinaryKey(),
                                            TargetValueEnvelope.decode(
                                                            replacementStore.get(
                                                                    ColumnFamily.TIMELINE, retryWork.ordinaryKey()),
                                                            TargetTimelineWorkRef.VALUE_TYPE)
                                                    .payload());
                                    assertEquals(1, uncertainMessage.runtime().admissionsUsed());
                                    assertEquals(0, uncertainMessage.runtime().uncertainRetryAdmissionsUsed());
                                }

                                assertEquals(1, uncertainMessage.runtime().attemptObligations().size());
                                assertEquals(
                                        AttemptLedgerState.UNCERTAIN,
                                        uncertainMessage.runtime().attemptObligations().getFirst().ledgerState());
                                assertArrayEquals(
                                        admission.attemptId(),
                                        uncertainMessage.runtime().attemptObligations().getFirst().publishAttemptId());
                                final var unknownBudget = TargetQuotaAttemptBudget.decode(TargetValueEnvelope.decode(
                                                replacementStore.get(ColumnFamily.META, outcomeBudgetKey),
                                                TargetQuotaAttemptBudget.VALUE_TYPE)
                                        .payload());
                                assertEquals(TargetQuotaAttemptBudget.Phase.UNKNOWN, unknownBudget.phase());
                                assertEquals(admittedBudget.commitment(), unknownBudget.commitment());
                                assertEquals(admittedBudget.allocated(), unknownBudget.allocated());
                                assertEquals(admittedBudget.revision() + 1, unknownBudget.revision());
                                if (!uncertainRetry) {
                                    final var outcomeOrder = TargetOrderState.decode(TargetValueEnvelope.decode(
                                                    replacementStore.get(ColumnFamily.META, admittedOrderKey),
                                                    TargetOrderState.VALUE_TYPE)
                                            .payload());
                                    assertEquals(
                                            TargetQueueState.nextRevision(orderAfterCancel.stateRevision()),
                                            outcomeOrder.stateRevision());
                                    assertArrayEquals(
                                            orderAfterCancel.lastAdmittedOrder().encodedKey(),
                                            outcomeOrder.lastAdmittedOrder().encodedKey());
                                    assertEquals(TargetOrderBarrier.fromMessage(uncertainMessage),
                                            outcomeOrder.barrier());
                                    assertNull(outcomeOrder.serviceableHead());
                                }
                                final var outcomeFirst = TargetResultRecord.decode(TargetValueEnvelope.decode(
                                                replacementStore.get(ColumnFamily.DEDUPE, systemKey(outcomeMutation)),
                                                TargetResultRecord.VALUE_TYPE)
                                        .payload());
                                final var outcomeResult = SystemMutationResult.decode(outcomeFirst.typedPayload());
                                assertEquals(ApplyStatus.APPLIED, outcomeResult.applyStatus());
                                assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, outcomeResult.stableCode());
                                assertArrayEquals(outcomeAt.canonicalBytes(), outcomeResult.appliedSourcePosition());
                                final byte[] outcomePositionKey = Bytes.concat(
                                        new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT},
                                        outcomeAt.canonicalBytes());
                                final var outcomePosition = TargetResultRecord.decode(TargetValueEnvelope.decode(
                                                replacementStore.get(ColumnFamily.DEDUPE, outcomePositionKey),
                                                TargetResultRecord.VALUE_TYPE)
                                        .payload());
                                outcomePosition.requireFirst(outcomeFirst);
                                final long outcomeVersionAfterApply = replacementStore.latestSequenceNumber();
                                final var outcomeAcknowledged = replacementWorker.runSourceTurn(
                                        new SchedulerBudget(64, 32L << 20, 60_000_000_000L), () -> 101);
                                assertEquals(
                                        SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED,
                                        outcomeAcknowledged.status(),
                                        () -> String.valueOf(outcomeAcknowledged.failure()));
                                assertEquals(outcomeEntry, outcomeAcknowledged.entry());
                                assertEquals(2, outcomeAcknowledgements.get());
                                assertTrue(replacementWorker.pendingSourceEntry().isEmpty());
                                assertEquals(beforeOutcomeMutation + 1, replacementStore.shardMutationSequence());
                                assertEquals(outcomeVersionAfterApply, replacementStore.latestSequenceNumber());
                                assertEquals(1, outcomeAuthorityResolutions.get());

                                if (uncertainRetry && !closedRetryQueue) {
                                    final var retryWork = uncertainMessage.runtime().timeline();
                                    final long due = retryWork.ordinaryEligibilityAtEpochMs();
                                    final var retryQueue = replacementWorker.readTargetQueue(
                                                    budget(), uncertainMessage.locator().target(), () -> 101)
                                            .orElseThrow().queue();
                                    final var retryHead = retryQueue.domains()
                                            .get(uncertainMessage.locator().domain().slot()).ordinaryHead();
                                    assertNotNull(retryHead);
                                    assertEquals(uncertainMessage.locator().messageId(), retryHead.messageId());
                                    assertEquals(due, retryHead.timeEpochMs());
                                    final var retryCost =
                                            replacementWorker.probeSelectedHead(budget(), retryHead, () -> 101);
                                    assertTrue(retryCost.deliverAtEpochMs() <= due
                                            && retryCost.expireAtEpochMs() > due);
                                    schedulerEpoch.set(due - 1);
                                    schedulerClaimGate.release();
                                    assertNull(schedulerClaims.poll(150, java.util.concurrent.TimeUnit.MILLISECONDS));
                                    schedulerEpoch.set(due);
                                    final var retryClaim =
                                            schedulerClaims.poll(5, java.util.concurrent.TimeUnit.SECONDS);
                                    assertNotNull(retryClaim,
                                            () -> "uncertain retry scheduler failure: " + ordinaryLoop.firstFailure());
                                    assertEquals(retryWork.locator(), retryClaim.work().locator());
                                    assertEquals(TimelineWorkKind.UNCERTAIN_RETRY, retryClaim.work().workKind());
                                    assertEquals(2, retryClaim.work().candidateAttemptNo());
                                    assertEquals(replacementOwnerIdentity[0], retryClaim.owner());
                                    schedulerClaimGate.release();
                                    final var retryBefore =
                                            (KafkaSourcePosition) replacementStore.appliedShardLogPosition();
                                    final var retryAt = source(
                                            retryBefore, retryBefore.offset() + 1,
                                            Math.max(retryBefore.brokerPersistenceTimeEpochMs() + 1, due));
                                    final var retryAdmission = targetAdmission(
                                            replacementStore, retryClaim, replacementOwnerIdentity[0], keys, retryAt);
                                    replacementNextSourceRecord.set(new SourceRecordConsumer.PolledSourceRecord(
                                            retryAdmission.entry(), (entry, outcome) -> {
                                                assertEquals(StableCode.OK,
                                                        outcome.systemMutationResult().stableCode());
                                                return SourceAcknowledgement.AcknowledgementResult.acked();
                                            }));
                                    final var retryTurn = replacementWorker.runSourceTurn(
                                            new SchedulerBudget(64, 32L << 20, 60_000_000_000L), () -> 101);
                                    assertEquals(SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED,
                                            retryTurn.status(),
                                            () -> String.valueOf(retryTurn.failure()));
                                    assertTargetAdmissionCommitted(replacementStore, retryClaim, retryAdmission);
                                    final var retried = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                                                    replacementStore.get(ColumnFamily.ID, admittedMessageKey),
                                                    TargetMessageRecord.VALUE_TYPE)
                                            .payload());
                                    assertEquals(GenerationAggregateState.UNCERTAIN, retried.aggregateState());
                                    assertEquals(2, retried.runtime().admissionsUsed());
                                    assertEquals(1, retried.runtime().uncertainRetryAdmissionsUsed());
                                    assertTrue(retried.runtime().possibleDestinationDuplicate());
                                    assertEquals(2, retried.runtime().attemptObligations().size());
                                    assertTrue(retried.runtime().attemptObligations().stream().anyMatch(ref ->
                                            Arrays.equals(ref.publishAttemptId(), admission.attemptId())
                                                    && ref.ledgerState() == AttemptLedgerState.UNCERTAIN));
                                    assertTrue(retried.runtime().attemptObligations().stream().anyMatch(ref ->
                                            Arrays.equals(ref.publishAttemptId(), retryAdmission.attemptId())
                                                    && ref.ledgerState() == AttemptLedgerState.PUBLISHING));
                                    final var oldBudgetAfterRetry =
                                            TargetQuotaAttemptBudget.decode(TargetValueEnvelope.decode(
                                                    replacementStore.get(ColumnFamily.META, outcomeBudgetKey),
                                                    TargetQuotaAttemptBudget.VALUE_TYPE)
                                            .payload());
                                    assertArrayEquals(unknownBudget.canonicalBytes(),
                                            oldBudgetAfterRetry.canonicalBytes());
                                    assertExhaustedRetryDoesNotWrite(
                                            replacementInitialized.backend(), replacementStore, otherScope,
                                            replacementInitialized.root().recoveryLineage(),
                                            admission, retryAdmission, replacementOwnerIdentity[0],
                                                otherRetryPolicy, keys);
                                    assertNull(ordinaryLoop.firstFailure());
                                    ordinaryLoop.close();
                                } else if (closedRetryQueue) {
                                    final var closedQueue = TargetQueueState.decode(TargetValueEnvelope.decode(
                                                    replacementStore.get(ColumnFamily.META,
                                                            TargetKeyCodec.state(uncertainMessage.locator().target())),
                                                    TargetQueueState.VALUE_TYPE)
                                            .payload());
                                    assertEquals(TargetQueueState.AdmissionState.CLOSED, closedQueue.admissionState());
                                    assertNull(uncertainMessage.runtime().timeline());
                                    assertTrue(closedQueue.domains().stream().allMatch(domain ->
                                            domain.ordinaryHead() == null && domain.nativeHead() == null));
                                    schedulerClaimGate.release();
                                    ordinaryLoop.close();
                                }

                            }

                            final var replacementBeforeReplay =
                                    (KafkaSourcePosition) replacementStore.appliedShardLogPosition();
                            final var replayAt = source(
                                    replacementBeforeReplay,
                                    replacementBeforeReplay.offset() + 1,
                                    replacementBeforeReplay.brokerLogAppendTimeEpochMs() + 1);
                            final var replayEntry = new SourceReplayRecord(otherSchedule, replayAt, null, null);
                            final var replacementAcknowledgements = new java.util.concurrent.atomic.AtomicInteger();
                            replacementNextSourceRecord.set(new SourceRecordConsumer.PolledSourceRecord(
                                    replayEntry,
                                    (entry, outcome) -> {
                                        assertEquals(replayEntry, entry);
                                        assertEquals(replayAt, outcome.position());
                                        assertNotNull(outcome.commandResult());
                                        return replacementAcknowledgements.incrementAndGet() == 1
                                                ? SourceAcknowledgement.AcknowledgementResult.unknown(null)
                                                : SourceAcknowledgement.AcknowledgementResult.acked();
                                    }));
                            final long mainSequenceBeforeUnknown = store.latestSequenceNumber();
                            final long replacementMutationsBeforeUnknown = replacementStore.shardMutationSequence();
                            final var unknownTurn = replacementWorker.runSourceTurn(
                                    new SchedulerBudget(64, 32L << 20, 60_000_000_000L), () -> 101);
                            assertEquals(SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN, unknownTurn.status());
                            assertEquals(replayEntry, unknownTurn.entry());
                            assertEquals(replayEntry, replacementWorker.pendingSourceEntry().orElseThrow());
                            assertEquals(1, replacementAcknowledgements.get());
                            assertFalse(replacementRuntime.fenced());
                            assertEquals(replayAt, replacementStore.appliedShardLogPosition());
                            assertEquals(
                                    replacementMutationsBeforeUnknown + 1,
                                    replacementStore.shardMutationSequence());

                            final long replacementSequenceAfterUnknown = replacementStore.latestSequenceNumber();
                            final var siblingBeforeReplay = (KafkaSourcePosition) store.appliedShardLogPosition();
                            final var siblingReplayAt = source(
                                    siblingBeforeReplay,
                                    siblingBeforeReplay.offset() + 1,
                                    siblingBeforeReplay.brokerLogAppendTimeEpochMs() + 1);
                            final var siblingReplayEntry =
                                    new SourceReplayRecord(schedule, siblingReplayAt, null, null);
                            final var siblingAcknowledgements = new java.util.concurrent.atomic.AtomicInteger();
                            claimNextSourceRecord.set(new SourceRecordConsumer.PolledSourceRecord(
                                    siblingReplayEntry,
                                    (entry, outcome) -> {
                                        assertEquals(siblingReplayEntry, entry);
                                        assertEquals(siblingReplayAt, outcome.position());
                                        assertNotNull(outcome.commandResult());
                                        return siblingAcknowledgements.incrementAndGet() == 1
                                                ? SourceAcknowledgement.AcknowledgementResult.unknown(null)
                                                : SourceAcknowledgement.AcknowledgementResult.acked();
                                    }));
                            final long mainMutationsBeforeSibling = store.shardMutationSequence();
                            final var actualFleet = new TargetWorkerShardFleetRuntime(
                                    workerClasses, resources, List.of(claimWorker, replacementWorker));
                            final var siblingApplyTurn = actualFleet.runNextSourceTurn(
                                    new SchedulerBudget(64, 32L << 20, 60_000_000_000L), () -> 101);
                            assertEquals(scope.shard(), siblingApplyTurn.shardId());
                            assertEquals(
                                    SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN,
                                    siblingApplyTurn.result().status());
                            assertEquals(siblingReplayEntry, siblingApplyTurn.result().entry());
                            assertEquals(1, siblingAcknowledgements.get());
                            assertEquals(siblingReplayEntry, claimWorker.pendingSourceEntry().orElseThrow());
                            assertFalse(claimRuntime.fenced());
                            assertFalse(replacementRuntime.fenced());
                            assertEquals(siblingReplayAt, store.appliedShardLogPosition());
                            assertTrue(store.latestSequenceNumber() > mainSequenceBeforeUnknown);
                            assertEquals(mainMutationsBeforeSibling + 1, store.shardMutationSequence());
                            final long mainSequenceAfterSibling = store.latestSequenceNumber();
                            assertEquals(replayEntry, replacementWorker.pendingSourceEntry().orElseThrow());
                            assertEquals(replacementSequenceAfterUnknown, replacementStore.latestSequenceNumber());

                            assertTrue(leases.release(replacementActive));
                            final long replacementMutationsBeforeLostOwnerRetry =
                                    replacementStore.shardMutationSequence();
                            final var lostOwnerRetry = actualFleet.runNextSourceTurn(
                                    new SchedulerBudget(64, 32L << 20, 60_000_000_000L), () -> 101);
                            assertEquals(otherShard, lostOwnerRetry.shardId());
                            assertEquals(
                                    SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN,
                                    lostOwnerRetry.result().status());
                            assertEquals(replayEntry, lostOwnerRetry.result().entry());
                            assertEquals(1, replacementAcknowledgements.get());
                            assertEquals(replayEntry, replacementWorker.pendingSourceEntry().orElseThrow());
                            assertTrue(replacementRuntime.fenced());
                            assertEquals(
                                    replacementMutationsBeforeUnknown + 1,
                                    replacementStore.shardMutationSequence());
                            assertEquals(replacementSequenceAfterUnknown, replacementStore.latestSequenceNumber());
                            assertEquals(
                                    replacementMutationsBeforeLostOwnerRetry,
                                    replacementStore.shardMutationSequence());
                            assertFalse(claimRuntime.fenced());
                            assertEquals(siblingReplayEntry, claimWorker.pendingSourceEntry().orElseThrow());

                            final var acknowledgedSiblingTurn = actualFleet.runNextSourceTurn(
                                    new SchedulerBudget(64, 32L << 20, 60_000_000_000L), () -> 101);
                            assertEquals(scope.shard(), acknowledgedSiblingTurn.shardId());
                            assertEquals(
                                    SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED,
                                    acknowledgedSiblingTurn.result().status());
                            assertEquals(siblingReplayEntry, acknowledgedSiblingTurn.result().entry());
                            assertEquals(2, siblingAcknowledgements.get());
                            assertTrue(claimWorker.pendingSourceEntry().isEmpty());
                            assertFalse(claimRuntime.fenced());
                            assertEquals(siblingReplayAt, store.appliedShardLogPosition());
                            assertEquals(mainSequenceAfterSibling, store.latestSequenceNumber());
                            assertEquals(mainMutationsBeforeSibling + 1, store.shardMutationSequence());
                            assertEquals(replacementSequenceAfterUnknown, replacementStore.latestSequenceNumber());
                            assertEquals(replayEntry, replacementWorker.pendingSourceEntry().orElseThrow());
                            assertTrue(replacementRuntime.fenced());

                            if (!rescheduled && !strictOrderExpiry) {
                                final var currentPosition = (KafkaSourcePosition) store.appliedShardLogPosition();
                                final var siblingBusinessAt = source(
                                        currentPosition,
                                        currentPosition.offset() + 1,
                                        currentPosition.brokerLogAppendTimeEpochMs() + 1);
                                final var overQuotaSchedule =
                                        schedule(intent, messageId, siblingBusinessAt, 7000);
                                expectedSiblingCommand.set(overQuotaSchedule);
                                final var overQuotaEntry = new SourceReplayRecord(
                                        overQuotaSchedule, siblingBusinessAt, null, null);
                                final var queueBeforeQuotaRejection =
                                        store.get(ColumnFamily.META, TargetKeyCodec.state(binding.target()));
                                final long sequenceBeforeQuotaRejection = store.latestSequenceNumber();
                                final var siblingBusinessAcks = new java.util.concurrent.atomic.AtomicInteger();
                                claimNextSourceRecord.set(new SourceRecordConsumer.PolledSourceRecord(
                                        overQuotaEntry,
                                        (entry, outcome) -> {
                                            assertEquals(overQuotaEntry, entry);
                                            assertEquals(siblingBusinessAt, outcome.position());
                                            assertEquals(
                                                    StableCode.HARD_QUOTA_EXCEEDED,
                                                    outcome.commandResult().stableCode());
                                            siblingBusinessAcks.incrementAndGet();
                                            return SourceAcknowledgement.AcknowledgementResult.acked();
                                        }));
                                final var siblingBusinessTurn = claimWorker.runSourceTurn(
                                        new SchedulerBudget(64, 32L << 20, 60_000_000_000L), () -> 101);
                                assertEquals(
                                        SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED,
                                        siblingBusinessTurn.status());
                                assertEquals(overQuotaEntry, siblingBusinessTurn.entry());
                                assertEquals(
                                        StableCode.HARD_QUOTA_EXCEEDED,
                                        siblingBusinessTurn.appliedOutcome().commandResult().stableCode());
                                assertEquals(1, siblingBusinessAcks.get());
                                assertTrue(claimWorker.pendingSourceEntry().isEmpty());
                                assertFalse(claimRuntime.fenced());
                                assertTrue(replacementRuntime.fenced());
                                assertEquals(siblingBusinessAt, store.appliedShardLogPosition());
                                assertTrue(store.latestSequenceNumber() > sequenceBeforeQuotaRejection);
                                assertNull(store.get(
                                        ColumnFamily.ID,
                                        TargetKeyCodec.message(overQuotaSchedule.delayMessageId())));
                                assertArrayEquals(
                                        queueBeforeQuotaRejection,
                                        store.get(ColumnFamily.META, TargetKeyCodec.state(binding.target())));
                            }
                        }

                        final long afterClaim = store.latestSequenceNumber();
                        assertNotEquals(
                                scanCut, claimWorker.readTargetQueueCut(budget(), replacementOwnerClock::get));
                        assertNotEquals(
                                partialPage.cut(),
                                claimWorker
                                        .scanTargetQueues(
                                                budget(), partialPage.nextAfter(), 1, replacementOwnerClock::get)
                                        .cut());
                        final var refreshed = claimWorker
                                .readTargetQueue(budget(), binding.target(), replacementOwnerClock::get)
                                .orElseThrow()
                                .queue();
                        assertNotEquals(actualQueue.headRevision(), refreshed.headRevision());
                        assertNotEquals(selectedHead, refreshed.domains().getFirst().ordinaryHead());
                        assertEquals(afterClaim, store.latestSequenceNumber());
                        assertThrows(
                                IllegalStateException.class,
                                () -> claimWorker.probeSelectedHead(
                                        budget(), selectedHead, replacementOwnerClock::get));
                        assertEquals(afterClaim, store.latestSequenceNumber());
                    } finally {
                        schedulerEpoch.set(0);
                        schedulerClaimGate.release();
                        ordinaryLoop.close();
                    }
                    assertTrue(ordinaryLoop.isClosed());
                    claimWorker.closeSource();
                }
            }
            final var before = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.ID, message.encodedKey()), TargetMessageRecord.VALUE_TYPE)
                    .payload());
            final var sourceBeforeFirstCommand = (KafkaSourcePosition) store.appliedShardLogPosition();
            final var at = source(
                    sourceBeforeFirstCommand,
                    sourceBeforeFirstCommand.offset() + 1,
                    sourceBeforeFirstCommand.brokerLogAppendTimeEpochMs() + 1);
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
            final var expiryResolutions = new java.util.concurrent.atomic.AtomicInteger();
            final var failWorkerReads = new java.util.concurrent.atomic.AtomicBoolean();
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
            final var expiryOwner = AuthorIdentity.owner(
                    Bytes.utf8("target-expiry-test-deployment"),
                    Bytes.utf8("target-expiry-test-worker"),
                    active.ownerEpoch(),
                    Bytes.sha256(active.leaseToken()));
            final TargetExpireGenerationVerifier.Authority expiryAuthority =
                    (actualScope, owner, mutation, position) -> {
                assertEquals(scope, actualScope);
                assertArrayEquals(expiryOwner.canonicalBytes(), owner.canonicalBytes());
                assertEquals(scope.shard(), mutation.shardId());
                assertEquals(position.shardId(), scope.shard());
                return new TargetExpireGenerationVerifier.Authorization(keys.getPublic(), 5, (a, b, c, proof) -> true);
            };
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
                                expiryResolutions.incrementAndGet();
                                return new TargetSourceApplyRuntime.ExpiryControl(
                                        expiryAuthority,
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
                                throw new AssertionError("unexpected membership control authority");
                            },
                            (a, b, c) -> guard(),
                            (a, b) -> {
                                if (failWorkerReads.get()) {
                                    throw new IllegalStateException("Target Worker read authority unavailable");
                                }
                                return guard();
                            },
                            entry -> {
                                resolutions.incrementAndGet();
                                return new TargetSourceApplyRuntime.CommandControl(
                                        policy,
                                        (reader, bound, source) -> {
                                            if (bound.commandType() == CommandType.SCHEDULE) {
                                                assertArrayEquals(
                                                        expectedBinding.canonicalBytes(), bound.canonicalBytes());
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
            final var loop = new WorkerSourceApplyLoop(consumer, workerClasses, runtime);
            if (strictOrderExpiry) {
                assertTrue(claimed);
                final var claimedMessage = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                                store.get(ColumnFamily.ID, TargetKeyCodec.message(messageId)),
                                TargetMessageRecord.VALUE_TYPE)
                        .payload());
                assertEquals(CurrentSendWorkKind.CLAIMED, claimedMessage.runtime().currentWorkKind());
                assertTrue(claim != null);
                final var orderKey = TargetKeyCodec.orderState(
                        claimedMessage.locator().target(), claimedMessage.locator().orderingDomain());
                final var orderBeforeExpiry = TargetOrderState.decode(TargetValueEnvelope.decode(
                                store.get(ColumnFamily.META, orderKey), TargetOrderState.VALUE_TYPE)
                        .payload());
                assertTrue(orderBeforeExpiry.barrier() != null);
                assertEquals(messageId, orderBeforeExpiry.barrier().locator().messageId());
                assertFalse(store.get(ColumnFamily.INFLIGHT, claim.key()) == null);
                assertFalse(store.get(ColumnFamily.META, claim.chargeKey()) == null);
                final var claimedExpiryIndexKey =
                        new TargetExpiryRef(claimedMessage.locator(), claimedMessage.expireAtEpochMs()).encodedKey();
                assertNull(store.get(ColumnFamily.TIMELINE, work.ordinaryKey()));
                assertFalse(store.get(ColumnFamily.TIMELINE, claimedExpiryIndexKey) == null);

                final var currentSource = (KafkaSourcePosition) store.appliedShardLogPosition();
                final var claimedExpiryAt = source(
                        currentSource,
                        currentSource.offset() + 1,
                        currentSource.brokerLogAppendTimeEpochMs() + 1);
                final var claimedExpiry = expire(
                        messageId,
                        claimedMessage.expireAtEpochMs(),
                        claimedExpiryAt,
                        expiryOwner,
                        keys,
                        false);
                final long beforeClaimedExpiryMutationSequence = store.shardMutationSequence();
                final var claimedExpiryResult = applyExpiry(loop, entries, claimedExpiry, claimedExpiryAt);
                assertEquals(StableCode.OK, claimedExpiryResult.stableCode());
                assertEquals(beforeClaimedExpiryMutationSequence + 1, store.shardMutationSequence());

                final var expiredClaimedMessage = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                                store.get(ColumnFamily.ID, TargetKeyCodec.message(messageId)),
                                TargetMessageRecord.VALUE_TYPE)
                        .payload());
                assertEquals(GenerationAggregateState.EXPIRED, expiredClaimedMessage.aggregateState());
                assertEquals(CurrentSendWorkKind.NONE, expiredClaimedMessage.runtime().currentWorkKind());
                assertNull(store.get(ColumnFamily.INFLIGHT, claim.key()));
                assertNull(store.get(ColumnFamily.META, claim.chargeKey()));
                final var orderAfterExpiry = TargetOrderState.decode(TargetValueEnvelope.decode(
                                store.get(ColumnFamily.META, orderKey), TargetOrderState.VALUE_TYPE)
                        .payload());
                assertNull(orderAfterExpiry.barrier());
                assertNull(store.get(ColumnFamily.TIMELINE, work.ordinaryKey()));
                if (work.nativeCandidate()) {
                    assertNull(store.get(ColumnFamily.TIMELINE, work.nativeKey()));
                }
                assertNull(store.get(ColumnFamily.TIMELINE, claimedExpiryIndexKey));
                assertEquals(claimedExpiryAt, store.appliedShardLogPosition());
                return;
            }
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
                if (after.runtime().timeline().nativeCandidate()) {
                    org.junit.jupiter.api.Assertions.assertNotNull(store.get(
                            ColumnFamily.TIMELINE, after.runtime().timeline().nativeKey()));
                }
                org.junit.jupiter.api.Assertions.assertNotNull(store.get(
                        ColumnFamily.TIMELINE,
                        new TargetExpiryRef(after.locator(), after.expireAtEpochMs()).encodedKey()));
            }
            assertNull(store.get(ColumnFamily.TIMELINE, work.ordinaryKey()));
            if (work.nativeCandidate()) {
                assertNull(store.get(ColumnFamily.TIMELINE, work.nativeKey()));
            }
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
            assertEquals(2 + (claimed && !rescheduled && !strictOrderExpiry ? 1 : 0), scheduleResolutions.get());
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
            assertEquals(2 + (claimed && !rescheduled && !strictOrderExpiry ? 1 : 0), scheduleResolutions.get());
            final var expiryAt =
                    source(conflictAt, conflictAt.offset() + 1, conflictAt.brokerLogAppendTimeEpochMs() + 1);
            final var targetExpiryStore = new TargetExpireGenerationStore(backend, scope, lineage, 16, 1);
            final var beforeExpiry = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.ID, TargetKeyCodec.message(fresh.delayMessageId())),
                            TargetMessageRecord.VALUE_TYPE)
                    .payload());
            final var expiry = expire(
                    fresh.delayMessageId(), beforeExpiry.expireAtEpochMs(), expiryAt, expiryOwner, keys, false);
            final var expiryDiscovery = new TargetExpiryDiscoveryStore(backend, scope);
            final var expiryProof = TargetExpireGenerationBody.decode(expiry.canonicalBody()).proof();
            final var expectedExpiry = new TargetExpiryDiscoveryStore.Candidate(
                    beforeExpiry.locator(), beforeExpiry.expireAtEpochMs());
            assertTrue(expiryProof.earliestEpochMs() > 0);
            final var beforeExpiryProof = new TrustedUtcIntervalEvidence(
                    expiryProof.earliestEpochMs() - 1,
                    expiryProof.latestEpochMs(),
                    expiryProof.source(),
                    expiryProof.sourceId(),
                    expiryProof.sourceConfigGeneration(),
                    expiryProof.sampleSequence(),
                    expiryProof.monotonicAnchorNs(),
                    expiryProof.sourceEvidenceSha256(),
                    expiryProof.sourceKeyVersion(),
                    expiryProof.sourceSignature());
            final long beforeDiscoverySequence = store.latestSequenceNumber();
            assertFalse(expiryDiscoveryFinds(expiryDiscovery, beforeExpiryProof, expectedExpiry));
            assertTrue(expiryDiscoveryFinds(expiryDiscovery, expiryProof, expectedExpiry));
            final var firstExpiryPage = expiryDiscovery.discover(budget(), null, expiryProof, (a, b) -> guard());
            assertTrue(firstExpiryPage.candidate().isPresent());
            assertThrows(
                    IllegalStateException.class,
                    () -> expiryDiscovery.discover(
                            budget(), firstExpiryPage.nextCursor(), beforeExpiryProof, (a, b) -> guard()));
            assertThrows(
                    com.nereusstream.delay.store.ReadIncompleteException.class,
                    () -> expiryDiscovery.discover(
                            new BoundedReadBudget(1, 1 << 20, 1_000_000_000L, System::nanoTime),
                            null,
                            expiryProof,
                            (a, b) -> guard()));
            assertThrows(
                    IllegalStateException.class,
                    () -> expiryDiscovery.discover(budget(), null, expiryProof, (a, b) -> {
                        throw new IllegalStateException("expiry discovery read authority unavailable");
                    }));
            assertEquals(beforeDiscoverySequence, store.latestSequenceNumber());
            final var messageExpiryWorker = TargetWorkerShardFactory.create(
                    () -> java.util.Optional.empty(),
                    runtime.acceptedAssignment(),
                    workerClasses,
                    store,
                    store.sharedResources(),
                    runtime,
                    new TargetWorkerShardRuntime.Maintenance(
                            new TargetCloseStore(backend, scope, lineage, 16, 1)
                                    .reservationControls((reader, bound) -> java.util.Optional.empty()),
                            new TargetReservationClosureWorkClassExecutor.Limits(
                                    4096, 250_000, 60_000_000_000L),
                            new TargetReservationExpiryWorkClassExecutor.Limits(
                                    2048, 100_000, 60_000_000_000L),
                            (a, b, c) -> guard(),
                            ignored -> {},
                            ignored -> {},
                            ignored -> {},
                            () -> 100));
            final var workerExpiryPage = messageExpiryWorker.discoverMessageExpiry(
                    budget(), null, expiryProof, () -> 100);
            assertEquals(expectedExpiry, workerExpiryPage.candidate().orElseThrow());
            final var missingExpiryMaintenance = assertThrows(
                    IllegalStateException.class,
                    () -> com.nereusstream.delay.ownership.TargetWorkerHostRuntime.start(
                            workerClasses,
                            store.sharedResources(),
                            List.of(messageExpiryWorker),
                            new SchedulerBudget(1, 1024, 1_000_000),
                            java.time.Duration.ofHours(1),
                            ignored -> {}));
            assertEquals(
                    "Target Worker message expiry maintenance must be configured before Host start",
                    missingExpiryMaintenance.getMessage());
            final var messageExpiryAppendOutcome = new java.util.concurrent.atomic.AtomicReference<>(
                    com.nereusstream.delay.ownership.ShardLogMutationAppender.AppendOutcome.persisted(expiryAt));
            final var messageExpiryAppendMutation = new java.util.concurrent.atomic.AtomicReference<>(expiry);
            final var messageExpiryHandoff = messageExpiryWorker.configureMessageExpiryMaintenance(
                    mutation -> {
                        final var expected = messageExpiryAppendMutation.get();
                        if (expected == null) {
                            messageExpiryAppendMutation.set(mutation);
                        } else {
                            assertEquals(expected, mutation);
                        }
                        return messageExpiryAppendOutcome.get();
                    },
                    () -> new TargetWorkerShardRuntime.MessageExpiryRequest(
                            budget(),
                            expiryProof,
                            expiry.retryUntilEpochMs(),
                            expiryOwner.asOwnerIdentity(),
                            expiry.signingKeyVersion(),
                            keys.getPrivate()));
            if (claimed) {
                final java.util.function.LongSupplier maintenanceThreadCount = () -> Thread.getAllStackTraces()
                        .keySet().stream()
                        .filter(thread -> thread.isAlive()
                                && thread.getName().equals("nereus-delay-target-maintenance"))
                        .count();
                final long maintenanceThreadsBeforeFailedStart = maintenanceThreadCount.getAsLong();
                assertThrows(
                        IllegalStateException.class,
                        () -> com.nereusstream.delay.ownership.TargetWorkerHostRuntime.start(
                                workerClasses,
                                store.sharedResources(),
                                List.of(messageExpiryWorker),
                                new SchedulerBudget(1, 1024, 1_000_000),
                                java.time.Duration.ofHours(1),
                                ignored -> {}));
                final long maintenanceThreadDeadline = System.nanoTime()
                        + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
                while (maintenanceThreadCount.getAsLong() > maintenanceThreadsBeforeFailedStart
                        && System.nanoTime() < maintenanceThreadDeadline) {
                    Thread.sleep(1);
                }
                assertTrue(maintenanceThreadCount.getAsLong() <= maintenanceThreadsBeforeFailedStart);
            }
            final var staleExpiryOwner = AuthorIdentity.owner(
                    Bytes.utf8("target-expiry-test-deployment"),
                    Bytes.utf8("target-expiry-test-worker"),
                    active.ownerEpoch(),
                    Bytes.sha256(Bytes.utf8("stale-target-expiry-lease-token")));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> messageExpiryHandoff.submit(
                            expectedExpiry,
                            expiryProof,
                            expiry.retryUntilEpochMs(),
                            staleExpiryOwner.asOwnerIdentity(),
                            expiry.signingKeyVersion(),
                            keys.getPrivate(),
                            () -> 100));
            assertEquals(beforeDiscoverySequence, store.latestSequenceNumber());
            final var messageExpirySubmission = messageExpiryHandoff.submit(
                    expectedExpiry,
                    expiryProof,
                    expiry.retryUntilEpochMs(),
                    expiryOwner.asOwnerIdentity(),
                    expiry.signingKeyVersion(),
                    keys.getPrivate(),
                    () -> 100);
            assertTrue(messageExpirySubmission.result().isEmpty());
            workerClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            final var messageExpiryResult = messageExpirySubmission.result().orElseThrow();
            assertEquals(TargetMessageExpiryWorkClassExecutor.ResultKind.APPENDED, messageExpiryResult.kind());
            assertEquals(expiryAt, messageExpiryResult.sourcePosition());
            assertEquals(expiry, messageExpirySubmission.mutation());
            assertEquals(beforeDiscoverySequence, store.latestSequenceNumber());
            assertTrue(messageExpiryHandoff.settlePending(() -> 100).isEmpty());
            final var checkpointCutFailure = assertThrows(
                    IllegalStateException.class,
                    () -> messageExpiryWorker.submitProtectedCheckpointCandidate(
                            null, null, null, null, null, null, null));
            assertEquals(
                    "Target checkpoint cannot cut a pending message expiry handoff",
                    checkpointCutFailure.getMessage());
            final var localCheckpointFailure = assertThrows(
                    IllegalStateException.class,
                    () -> messageExpiryWorker.submitLocalCheckpointCandidate(
                            null, null, null, null, null, null, null));
            assertEquals(
                    "Target checkpoint cannot cut a pending message expiry handoff",
                    localCheckpointFailure.getMessage());
            final var pauseFailure = assertThrows(
                    IllegalStateException.class, messageExpiryWorker::pauseNewTurns);
            assertEquals("Target Worker cannot pause a pending message expiry handoff", pauseFailure.getMessage());
            assertEquals(
                    messageExpirySubmission.task(),
                    messageExpiryWorker
                            .settlePendingMessageExpiryForDrain(
                                    new SchedulerBudget(100, 2_000_000, 60_000_000_000L),
                                    new SchedulerBudget(1, 1, 60_000_000_000L),
                                    () -> 100)
                            .orElseThrow());
            assertThrows(
                    IllegalStateException.class,
                    () -> messageExpiryHandoff.submit(
                            expectedExpiry,
                            expiryProof,
                            expiry.retryUntilEpochMs(),
                            expiryOwner.asOwnerIdentity(),
                            expiry.signingKeyVersion(),
                            keys.getPrivate(),
                            () -> 100));
            assertEquals(beforeDiscoverySequence, store.latestSequenceNumber());
            final long beforeExpirySequence = store.latestSequenceNumber();
            final long beforeExpiryMutationSequence = store.shardMutationSequence();
            final var beforeExpirySource = store.appliedShardLogPosition();
            final var rejectedExpiry = targetExpiryStore.prepareFirst(budget(), expiry, expiryAt, expiryAuthority);
            assertThrows(
                    IllegalStateException.class,
                    () -> targetExpiryStore.commit(rejectedExpiry, (a, b, c) -> {
                        throw new IllegalStateException("expiry physical capacity unavailable");
                    }));
            assertEquals(beforeExpirySequence, store.latestSequenceNumber());
            assertEquals(beforeExpiryMutationSequence, store.shardMutationSequence());
            assertEquals(beforeExpirySource, store.appliedShardLogPosition());
            assertArrayEquals(
                    beforeExpiry.canonicalBytes(),
                    TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.ID, beforeExpiry.encodedKey()),
                                    TargetMessageRecord.VALUE_TYPE)
                            .payload());
            assertNull(store.get(
                    ColumnFamily.DEDUPE,
                    Bytes.concat(
                            new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT},
                            expiry.systemMutationId())));
            assertEquals(
                    StableCode.OK,
                    applyExpiry(loop, entries, messageExpirySubmission.mutation(), expiryAt).stableCode());
            assertTrue(messageExpiryWorker
                    .settlePendingMessageExpiryForDrain(
                            new SchedulerBudget(100, 2_000_000, 60_000_000_000L),
                            new SchedulerBudget(1, 1, 60_000_000_000L),
                            () -> 100)
                    .isEmpty());
            assertEquals(
                    TargetMessageExpiryWorkClassExecutor.ResultKind.APPLIED,
                    messageExpirySubmission.result().orElseThrow().kind());
            final var expired = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.ID, beforeExpiry.encodedKey()), TargetMessageRecord.VALUE_TYPE)
                    .payload());
            assertEquals(GenerationAggregateState.EXPIRED, expired.aggregateState());
            assertEquals(CurrentSendWorkKind.NONE, expired.runtime().currentWorkKind());
            assertEquals(beforeExpiry.stateVersion() + 1, expired.stateVersion());
            assertNull(store.get(ColumnFamily.TIMELINE, beforeExpiry.runtime().timeline().ordinaryKey()));
            if (beforeExpiry.runtime().timeline().nativeCandidate()) {
                assertNull(store.get(ColumnFamily.TIMELINE, beforeExpiry.runtime().timeline().nativeKey()));
            }
            assertNull(store.get(
                    ColumnFamily.TIMELINE,
                    new TargetExpiryRef(beforeExpiry.locator(), beforeExpiry.expireAtEpochMs()).encodedKey()));
            final var expiryOwnerRecord = TargetQuotaPayloadOwner.decode(TargetValueEnvelope.decode(
                            store.get(
                                    ColumnFamily.META,
                                    Bytes.concat(
                                            new byte[] {
                                                TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG,
                                                TargetKeyCodec.KEY_FORMAT
                                            },
                                            fresh.delayMessageId().bytes())),
                            TargetQuotaPayloadOwner.VALUE_TYPE)
                    .payload());
            assertEquals(TargetQuotaPayloadOwner.Phase.RETAINED, expiryOwnerRecord.phase());
            final var expiryTerminal = TargetTerminalGenerationRecord.decode(TargetValueEnvelope.decode(
                            store.get(
                                    ColumnFamily.TERMINAL,
                                    TargetTerminalGenerationRecord.key(beforeExpiry.locator())),
                            TargetTerminalGenerationRecord.VALUE_TYPE)
                    .payload());
            assertEquals(StableCode.ALREADY_EXPIRED, expiryTerminal.terminalCode());
            expiryTerminal.requireOwner(expiryOwnerRecord);
            assertEquals(beforeExpiryMutationSequence + 1, store.shardMutationSequence());
            assertEquals(expiryAt, store.appliedShardLogPosition());
            final long afterExpiry = store.latestSequenceNumber();
            assertEquals(StableCode.OK, applyExpiry(loop, entries, expiry, expiryAt).stableCode());
            assertEquals(afterExpiry, store.latestSequenceNumber());
            assertEquals(1, expiryResolutions.get());
            final var laterExpiryDuplicateAt =
                    source(expiryAt, expiryAt.offset() + 1, expiryAt.brokerLogAppendTimeEpochMs() + 1);
            assertEquals(
                    StableCode.OK,
                    applyExpiry(loop, entries, expiry, laterExpiryDuplicateAt).stableCode());
            assertEquals(beforeExpiryMutationSequence + 2, store.shardMutationSequence());
            assertEquals(1, expiryResolutions.get());
            final var unauthorizedAt = source(
                    laterExpiryDuplicateAt,
                    laterExpiryDuplicateAt.offset() + 1,
                    laterExpiryDuplicateAt.brokerLogAppendTimeEpochMs() + 1);
            final var unauthorizedExpiry = expire(
                    fresh.delayMessageId(),
                    beforeExpiry.expireAtEpochMs(),
                    unauthorizedAt,
                    expiryOwner,
                    keys,
                    true);
            final var unauthorizedResult = applyExpiry(loop, entries, unauthorizedExpiry, unauthorizedAt);
            assertEquals(ApplyStatus.REJECTED, unauthorizedResult.applyStatus());
            assertEquals(StableCode.UNAUTHORIZED_SYSTEM_MUTATION, unauthorizedResult.stableCode());
            assertEquals(2, expiryResolutions.get());
            final var replacementAt = source(
                    unauthorizedAt, unauthorizedAt.offset() + 1, unauthorizedAt.brokerLogAppendTimeEpochMs() + 1);
            final var replacementId = new DelayMessageId(
                    cancel(fresh.delayMessageId(), replacementAt, 2020).commandId().bytes());
            final var replacement = schedule(intent, replacementId, replacementAt, 2021);
            assertEquals(
                    StableCode.SCHEDULED,
                    apply(loop, entries, replacement, replacementAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            final var replacementMessage = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.ID, TargetKeyCodec.message(replacement.delayMessageId())),
                            TargetMessageRecord.VALUE_TYPE)
                    .payload());
            final var replacementExpiryCandidate = new TargetExpiryDiscoveryStore.Candidate(
                    replacementMessage.locator(), replacementMessage.expireAtEpochMs());
            final var replacementExpiryPage = messageExpiryWorker.discoverMessageExpiry(
                    budget(), null, expiryProof, () -> 100);
            assertEquals(replacementExpiryCandidate, replacementExpiryPage.candidate().orElseThrow());
            final var replacementExpiryAt =
                    source(replacementAt, replacementAt.offset() + 1, replacementAt.brokerLogAppendTimeEpochMs() + 1);
            final var replacementExpiry = expire(
                    replacement.delayMessageId(),
                    replacementMessage.expireAtEpochMs(),
                    replacementExpiryAt,
                    expiryOwner,
                    keys,
                    false);
            final long beforeReplacementExpirySubmit = store.latestSequenceNumber();
            messageExpiryAppendMutation.set(replacementExpiry);
            messageExpiryAppendOutcome.set(
                    com.nereusstream.delay.ownership.ShardLogMutationAppender.AppendOutcome.unknown());
            final var replacementExpirySubmission = messageExpiryHandoff.submit(
                    replacementExpiryCandidate,
                    expiryProof,
                    replacementExpiry.retryUntilEpochMs(),
                    expiryOwner.asOwnerIdentity(),
                    expiry.signingKeyVersion(),
                    keys.getPrivate(),
                    () -> 100);
            workerClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            assertEquals(
                    TargetMessageExpiryWorkClassExecutor.ResultKind.UNKNOWN,
                    replacementExpirySubmission.result().orElseThrow().kind());
            assertNull(replacementExpirySubmission.result().orElseThrow().sourcePosition());
            assertEquals(replacementExpiry, replacementExpirySubmission.result().orElseThrow().mutation());
            assertFalse(runtime.fenced());
            assertEquals(beforeReplacementExpirySubmit, store.latestSequenceNumber());
            failWorkerReads.set(true);
            final var readAuthorityFailure = assertThrows(
                    IllegalStateException.class,
                    () -> messageExpiryHandoff.submit(
                            replacementExpiryCandidate,
                            expiryProof,
                            replacementExpiry.retryUntilEpochMs(),
                            expiryOwner.asOwnerIdentity(),
                            expiry.signingKeyVersion(),
                            keys.getPrivate(),
                            () -> 100));
            assertEquals("Target Worker read authority unavailable", readAuthorityFailure.getMessage());
            failWorkerReads.set(false);
            assertEquals(beforeReplacementExpirySubmit, store.latestSequenceNumber());
            assertTrue(messageExpiryHandoff.settlePending(() -> 100).isEmpty());
            assertThrows(
                    IllegalStateException.class,
                    () -> messageExpiryHandoff.submit(
                            replacementExpiryCandidate,
                            expiryProof,
                            replacementExpiry.retryUntilEpochMs(),
                            expiryOwner.asOwnerIdentity(),
                            expiry.signingKeyVersion(),
                            keys.getPrivate(),
                            () -> 100));
            assertEquals(beforeReplacementExpirySubmit, store.latestSequenceNumber());
            assertEquals(
                    StableCode.OK,
                    applyExpiry(loop, entries, replacementExpiry, replacementExpiryAt).stableCode());
            final var reconciledReplacement = messageExpiryHandoff.settlePending(() -> 100).orElseThrow();
            assertEquals(
                    TargetMessageExpiryWorkClassExecutor.ResultKind.APPLIED, reconciledReplacement.kind());
            assertEquals(replacementExpiry, reconciledReplacement.mutation());
            assertEquals(
                    TargetMessageExpiryWorkClassExecutor.ResultKind.APPLIED,
                    replacementExpirySubmission.result().orElseThrow().kind());
            assertNull(replacementExpirySubmission.result().orElseThrow().sourcePosition());
            final var producerScheduleAt = source(
                    replacementExpiryAt,
                    replacementExpiryAt.offset() + 1,
                    replacementExpiryAt.brokerLogAppendTimeEpochMs() + 1);
            final var producerMessageId = new DelayMessageId(
                    cancel(replacement.delayMessageId(), producerScheduleAt, 2022).commandId().bytes());
            final var producerSchedule = schedule(intent, producerMessageId, producerScheduleAt, 2023);
            assertEquals(
                    StableCode.SCHEDULED,
                    apply(loop, entries, producerSchedule, producerScheduleAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            final var producerMessage = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.ID, TargetKeyCodec.message(producerSchedule.delayMessageId())),
                            TargetMessageRecord.VALUE_TYPE)
                    .payload());
            final var producerCandidate = new TargetExpiryDiscoveryStore.Candidate(
                    producerMessage.locator(), producerMessage.expireAtEpochMs());
            final var producerExpiryAt = source(
                    producerScheduleAt,
                    producerScheduleAt.offset() + 1,
                    producerScheduleAt.brokerLogAppendTimeEpochMs() + 1);
            messageExpiryAppendMutation.set(null);
            messageExpiryAppendOutcome.set(
                    com.nereusstream.delay.ownership.ShardLogMutationAppender.AppendOutcome.persisted(
                            producerExpiryAt));
            final long beforeScheduledExpiry = store.latestSequenceNumber();
            final var scheduledExpiryTask = messageExpiryWorker.runMessageExpiryMaintenanceTurn().orElseThrow();
            assertEquals(WorkClass.EXPIRY, scheduledExpiryTask.workClass());
            assertEquals(beforeScheduledExpiry, store.latestSequenceNumber());
            workerClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            final var scheduledExpiryMutation = messageExpiryAppendMutation.get();
            assertTrue(scheduledExpiryMutation != null);
            final var scheduledExpiryBody = TargetExpireGenerationBody.decode(scheduledExpiryMutation.canonicalBody());
            assertEquals(producerCandidate.locator().messageId(), scheduledExpiryBody.messageId());
            assertEquals(
                    scheduledExpiryTask,
                    messageExpiryWorker.runMessageExpiryMaintenanceTurn().orElseThrow());
            assertEquals(beforeScheduledExpiry, store.latestSequenceNumber());
            assertEquals(
                    StableCode.OK,
                    applyExpiry(loop, entries, scheduledExpiryMutation, producerExpiryAt).stableCode());
            assertTrue(messageExpiryWorker.runMessageExpiryMaintenanceTurn().isEmpty());
            assertTrue(messageExpiryHandoff.settlePending(() -> 100).isEmpty());

            final var nextScheduleAt = source(
                    producerExpiryAt,
                    producerExpiryAt.offset() + 1,
                    producerExpiryAt.brokerLogAppendTimeEpochMs() + 1);
            final var nextMessageId = new DelayMessageId(
                    cancel(producerSchedule.delayMessageId(), nextScheduleAt, 2024).commandId().bytes());
            final var nextSchedule = schedule(intent, nextMessageId, nextScheduleAt, 2025);
            assertEquals(
                    StableCode.SCHEDULED,
                    apply(loop, entries, nextSchedule, nextScheduleAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            final var nextMessage = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.ID, TargetKeyCodec.message(nextSchedule.delayMessageId())),
                            TargetMessageRecord.VALUE_TYPE)
                    .payload());
            final var nextExpiryCandidate = new TargetExpiryDiscoveryStore.Candidate(
                    nextMessage.locator(), nextMessage.expireAtEpochMs());
            final var nextExpiryPage = messageExpiryWorker.discoverMessageExpiry(
                    budget(), null, expiryProof, () -> 100);
            assertEquals(nextExpiryCandidate, nextExpiryPage.candidate().orElseThrow());
            final var nextExpiryAt = source(
                    nextScheduleAt,
                    nextScheduleAt.offset() + 1,
                    nextScheduleAt.brokerLogAppendTimeEpochMs() + 1);
            final var nextExpiry = expire(
                    nextSchedule.delayMessageId(),
                    nextMessage.expireAtEpochMs(),
                    nextExpiryAt,
                    expiryOwner,
                    keys,
                    false);
            messageExpiryAppendMutation.set(nextExpiry);
            messageExpiryAppendOutcome.set(
                    com.nereusstream.delay.ownership.ShardLogMutationAppender.AppendOutcome.definitelyNotPersisted());
            final long beforeNextExpirySubmit = store.latestSequenceNumber();
            final var nextExpirySubmission = messageExpiryHandoff.submit(
                    nextExpiryCandidate,
                    expiryProof,
                    nextExpiry.retryUntilEpochMs(),
                    expiryOwner.asOwnerIdentity(),
                    expiry.signingKeyVersion(),
                    keys.getPrivate(),
                    () -> 100);
            workerClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            assertEquals(
                    TargetMessageExpiryWorkClassExecutor.ResultKind.DEFINITIVELY_NOT_APPENDED,
                    nextExpirySubmission.result().orElseThrow().kind());
            assertEquals(beforeNextExpirySubmit, store.latestSequenceNumber());
            assertEquals(
                    TargetMessageExpiryWorkClassExecutor.ResultKind.DEFINITIVELY_NOT_APPENDED,
                    messageExpiryHandoff.settlePending(() -> 100).orElseThrow().kind());
            final var rejectedExpiryAt = source(
                    nextScheduleAt,
                    nextScheduleAt.offset() + 1,
                    nextScheduleAt.brokerLogAppendTimeEpochMs() + 1);
            final var invalidProofExpiry = expire(
                    nextSchedule.delayMessageId(),
                    nextMessage.expireAtEpochMs(),
                    rejectedExpiryAt,
                    expiryOwner,
                    wrongProofKeys,
                    false);
            messageExpiryAppendMutation.set(invalidProofExpiry);
            messageExpiryAppendOutcome.set(
                    com.nereusstream.delay.ownership.ShardLogMutationAppender.AppendOutcome.persisted(
                    rejectedExpiryAt));
            final var rejectedExpirySubmission = messageExpiryHandoff.submit(
                    nextExpiryCandidate,
                    expiryProof,
                    invalidProofExpiry.retryUntilEpochMs(),
                    expiryOwner.asOwnerIdentity(),
                    expiry.signingKeyVersion(),
                    wrongProofKeys.getPrivate(),
                    () -> 100);
            workerClasses.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
            assertEquals(
                    TargetMessageExpiryWorkClassExecutor.ResultKind.APPENDED,
                    rejectedExpirySubmission.result().orElseThrow().kind());
            final var rejectedExpiryResult = applyExpiry(loop, entries, invalidProofExpiry, rejectedExpiryAt);
            assertEquals(ApplyStatus.REJECTED, rejectedExpiryResult.applyStatus());
            assertEquals(StableCode.UNAUTHORIZED_SYSTEM_MUTATION, rejectedExpiryResult.stableCode());
            final long afterRejectedExpiry = store.latestSequenceNumber();
            assertThrows(
                    IllegalStateException.class,
                    () -> messageExpiryHandoff.submit(
                            nextExpiryCandidate,
                            expiryProof,
                            invalidProofExpiry.retryUntilEpochMs(),
                            expiryOwner.asOwnerIdentity(),
                            expiry.signingKeyVersion(),
                            wrongProofKeys.getPrivate(),
                            () -> 100));
            assertEquals(afterRejectedExpiry, store.latestSequenceNumber());
            final var prepareAt = source(
                    rejectedExpiryAt,
                    rejectedExpiryAt.offset() + 1,
                    rejectedExpiryAt.brokerLogAppendTimeEpochMs() + 1);
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
            assertEquals(6 + (claimed && !rescheduled && !strictOrderExpiry ? 1 : 0), scheduleResolutions.get());
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
            if (!claimed) {
                final var cleanupRuntime = new TargetSourceApplyRuntime(
                        initialized,
                        store,
                        assignment,
                        active,
                        new TargetSourceApplyRuntime.Authorities(
                            leases,
                            SourceReplaySuccessor.strictKafka(),
                            entry -> { throw new AssertionError("cleanup Worker resolved a grant"); },
                            entry -> { throw new AssertionError("cleanup Worker resolved a fence"); },
                            entry -> { throw new AssertionError("cleanup Worker resolved expiry"); },
                            entry -> { throw new AssertionError("cleanup Worker resolved Close"); },
                            entry -> { throw new AssertionError("cleanup Worker resolved membership"); },
                            (a, b, c) -> guard(),
                            (a, b) -> guard(),
                            entry -> { throw new AssertionError("cleanup Worker resolved a command"); }),
                        new TargetSourceApplyRuntime.Limits(4096, 32L << 20, 60_000_000_000L, 16, 1),
                        System::nanoTime);
                final var pausedWorker = TargetWorkerShardFactory.create(
                        () -> java.util.Optional.empty(),
                        cleanupRuntime.acceptedAssignment(),
                        workerClasses,
                        store,
                        resources,
                        cleanupRuntime,
                        new TargetWorkerShardRuntime.Maintenance(
                                new TargetCloseStore(backend, scope, lineage, 16, 1)
                                        .reservationControls((reader, bound) -> java.util.Optional.empty()),
                                new TargetReservationClosureWorkClassExecutor.Limits(
                                        4096, 250_000, 60_000_000_000L),
                                new TargetReservationExpiryWorkClassExecutor.Limits(
                                        2048, 100_000, 60_000_000_000L),
                                (a, b, c) -> guard(),
                                ignored -> {},
                                ignored -> {},
                                ignored -> {},
                                () -> 100));
                pausedWorker.pauseNewTurns();
                final var cleanupHost = TargetWorkerHostTestBridge.withoutMaintenanceTimer(
                        workerClasses, resources, List.of(pausedWorker));
                final var subscriptionClosed = new java.util.concurrent.atomic.AtomicBoolean();
                final TargetWorkerOrdinaryDrr.Requests subscribedRequests = new TargetWorkerOrdinaryDrr.Requests() {
                    @Override
                    public java.util.Optional<TargetWorkerOrdinaryDrr.Request> resolve(
                            final TargetWorkerShardRuntime shard,
                            final com.nereusstream.delay.runtime.TargetHeadCostProbe.Cost cost) {
                        return java.util.Optional.empty();
                    }

                    @Override
                    public java.io.Closeable subscribeNativePolicyChanges(final Runnable wakeup) {
                        return () -> subscriptionClosed.set(true);
                    }
                };
                final var cacheConfigurationFailure = assertThrows(
                        IllegalStateException.class,
                        () -> cleanupHost.startOrdinaryScheduling(
                                new TargetWorkerTargetInventory.Limits(
                                        1, 16, 4, 8, 4096, 32L << 20, 60_000_000_000L),
                                new TargetWorkerOrdinaryDrr.Limits(
                                        100, 200, 200, 16, 4096, 32L << 20, 60_000_000_000L),
                                new SchedulerBudget(100, 2_000_000, 60_000_000_000L),
                                java.time.Duration.ofSeconds(10),
                                subscribedRequests,
                                ignored -> {},
                                () -> 100,
                                () -> 100,
                                System::nanoTime,
                                ignored -> {}));
                assertEquals(
                        "Target Worker source and maintenance admission is paused",
                        cacheConfigurationFailure.getMessage());
                assertTrue(subscriptionClosed.get());
            }

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
            final var priorSource = (KafkaSourcePosition) store.appliedShardLogPosition();
            final var uncertainAt = source(
                    priorSource, priorSource.offset() + 1, priorSource.brokerLogAppendTimeEpochMs() + 1);
            final long closeThrough = fifthReservation.expiryEpochMs() + 1;
            final var uncertainProof = new TrustedUtcIntervalEvidence(
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
            final var uncertainBody = new TargetTimeFenceBody(
                    scope.shard(), closeThrough + 1000, closeThrough, 1, uncertainProof);
            final var uncertainMutation = SystemMutation.signed(
                    scope.shard(),
                    SystemMutationType.TIME_FENCE,
                    uncertainBody.retryUntil(),
                    uncertainBody.proofId(),
                    uncertainBody.canonicalBytes(),
                    AuthorIdentity.fence(bytes(32, 0x93), 1).canonicalBytes(),
                    1,
                    keys.getPrivate());
            final var uncertainEntry = new SourceReplayMutation(uncertainMutation, uncertainAt, null, null);
            uncertainEntryForRecovery[0] = uncertainEntry;
            final var unknownAcknowledgements = new java.util.concurrent.atomic.AtomicInteger();
            entries.add(new SourceRecordConsumer.PolledSourceRecord(uncertainEntry, (entry, outcome) -> {
                assertEquals(uncertainEntry, entry);
                assertEquals(StableCode.OK, outcome.systemMutationResult().stableCode());
                unknownAcknowledgements.incrementAndGet();
                return SourceAcknowledgement.AcknowledgementResult.unknown(null);
            }));
            final long beforeUncertainApply = store.shardMutationSequence();
            if (!claimed && !rescheduled && !strictOrderExpiry) {
                TargetStoreBackendFailureTestBridge.failNextCommitAfterNativeWrite(backend);
                final var uncertainTurn = loop.runTurn(new SchedulerBudget(1, 1_000_000, 1_000), () -> 100);
                assertEquals(SourceApplyCoordinator.TurnStatus.APPLY_FAILURE, uncertainTurn.status());
                assertEquals(uncertainEntry, uncertainTurn.entry());
                assertEquals(uncertainEntry, loop.pendingEntry().orElseThrow());
                assertEquals(0, unknownAcknowledgements.get());
                assertTrue(store.isWriteOutcomeUncertain());
                assertTrue(runtime.fenced());
                assertTrue(leases.release(active));
            } else {
                final var uncertainTurn = loop.runTurn(new SchedulerBudget(1, 1_000_000, 1_000), () -> 100);
                assertEquals(SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN, uncertainTurn.status());
                assertEquals(uncertainEntry, loop.pendingEntry().orElseThrow());
                assertEquals(1, unknownAcknowledgements.get());
                assertEquals(beforeUncertainApply + 1, store.shardMutationSequence());
                assertEquals(uncertainAt, store.appliedShardLogPosition());

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
            }
            // Simulate process loss; the source fixture replays this unacknowledged entry after reopen.
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
        try (var resources = WorkerRuntimeTestSupport.openWithSyntheticObservation(config);
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
                            entry -> { throw new AssertionError("unexpected Target expiry authority"); },
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
            final var replayPolls = new java.util.concurrent.atomic.AtomicInteger();
            final var replayAcknowledgements = new java.util.concurrent.atomic.AtomicInteger();
            final SourceReplayMutation replayedEntry = java.util.Objects.requireNonNull(
                    uncertainEntryForRecovery[0], "uncertain source entry");
            final SourceRecordConsumer simulatedDurableSource = new SourceRecordConsumer() {
                @Override
                public java.util.Optional<PolledSourceRecord> poll() {
                    if (replayPolls.getAndIncrement() != 0) {
                        return java.util.Optional.empty();
                    }
                    return java.util.Optional.of(new PolledSourceRecord(replayedEntry, (entry, outcome) -> {
                        assertEquals(replayedEntry, entry);
                        assertEquals(StableCode.OK, outcome.systemMutationResult().stableCode());
                        replayAcknowledgements.incrementAndGet();
                        return SourceAcknowledgement.AcknowledgementResult.acked();
                    }));
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
            final var reopenedWorker = TargetWorkerShardFactory.create(
                    simulatedDurableSource,
                    reopenedRuntime.acceptedAssignment(),
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
            final long beforeRecoverySequence = reopened.latestSequenceNumber();
            final long beforeRecoveryMutation = reopened.shardMutationSequence();
            final var recoveredSourceTurn = reopenedFleet.runNextSourceTurn(
                    new SchedulerBudget(1, 1_000_000, 60_000_000_000L), () -> 101);
            assertEquals(
                    SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED,
                    recoveredSourceTurn.result().status());
            assertEquals(replayedEntry, recoveredSourceTurn.result().entry());
            assertEquals(
                    StableCode.OK,
                    recoveredSourceTurn.result().appliedOutcome().systemMutationResult().stableCode());
            assertEquals(1, replayPolls.get());
            assertEquals(1, replayAcknowledgements.get());
            assertEquals(beforeRecoverySequence, reopened.latestSequenceNumber());
            assertEquals(beforeRecoveryMutation, reopened.shardMutationSequence());
            assertEquals(replayedEntry.position(), reopened.appliedShardLogPosition());
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
            assertThrows(
                    IllegalStateException.class,
                    () -> reopenedWorker.scanTargetQueues(budget(), null, 1, () -> 101));
            assertThrows(IllegalStateException.class, () -> reopenedWorker.readTargetQueue(budget(), null, () -> 101));
            assertThrows(IllegalStateException.class, () -> reopenedWorker.readTargetQueueCut(budget(), () -> 101));
            assertThrows(
                    IllegalStateException.class, () -> reopenedWorker.probeSelectedHead(budget(), null, () -> 101));
            assertThrows(
                    IllegalStateException.class,
                    () -> reopenedWorker.claim(
                            budget(),
                            null,
                            new OwnerIdentity(
                                    bytes(16, 0x72),
                                    bytes(16, 0x73),
                                    activeReopened.ownerEpoch(),
                                    activeReopened.leaseToken()),
                            101,
                            1_000,
                            100,
                            bytes(32, 0x75),
                            (kind, delta) -> {},
                            (a, b, c) -> guard(),
                            () -> 101));
            assertEquals(beforePausedGc, reopened.latestSequenceNumber());
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

    private record TargetAdmissionFixture(SourceReplayMutation entry, byte[] attemptId) {
        private TargetAdmissionFixture {
            attemptId = Bytes.copy(attemptId);
        }

        @Override
        public byte[] attemptId() {
            return Bytes.copy(attemptId);
        }
    }

    private static TargetAdmissionFixture targetAdmission(
            ShardStore store,
            TargetClaimRecord claim,
            OwnerIdentity owner,
            KeyPair keys,
            KafkaSourcePosition at) {
        return targetAdmission(store, claim, owner, keys, at, null, null);
    }

    private static TargetAdmissionFixture targetAdmission(
            ShardStore store,
            TargetClaimRecord claim,
            OwnerIdentity owner,
            KeyPair keys,
            KafkaSourcePosition at,
            com.nereusstream.delay.protocol.TargetChannelIdentity channel,
            ProfileRef capabilityProfile) {
        final byte[] messageKey = TargetKeyCodec.message(claim.work().locator().messageId());
        final var claimed = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                        store.get(ColumnFamily.ID, messageKey), TargetMessageRecord.VALUE_TYPE)
                .payload());
        assertEquals(CurrentSendWorkKind.CLAIMED, claimed.runtime().currentWorkKind());
        assertArrayEquals(claim.claimId(), claimed.runtime().claimId());
        final long retryUntil = Math.addExact(at.brokerPersistenceTimeEpochMs(), 10_000);
        final long decisionEarliest =
                Math.max(at.brokerPersistenceTimeEpochMs(), claim.work().retryEligibilityAtEpochMs());
        assertTrue(decisionEarliest + 1 < claimed.expireAtEpochMs());
        final var decisionTime = new TrustedUtcIntervalEvidence(
                decisionEarliest,
                decisionEarliest + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                bytes(32, 0x96),
                1,
                1,
                1,
                bytes(32, 0x97),
                0,
                new byte[0]);
        final int attemptNo = claim.work().candidateAttemptNo();
        final byte[] attemptId = SystemMutation.computePublishAttemptLogicalIdentity(
                claim.claimId(),
                claim.work().locator().messageId(),
                Integer.toUnsignedLong(claim.work().locator().generation()),
                Integer.toUnsignedLong(attemptNo));
        final var obligation = new AttemptObligationRef(
                attemptId,
                claim.work().locator().generation(),
                AttemptLedgerState.PUBLISHING,
                KeyCodec.inflight((byte) 2, owner.ownerEpoch(), attemptId));
        final long[] reserved = new long[CapacityDimension.COUNT];
        reserved[CapacityDimension.RESULT_BYTES.wireValue() - 1] = 128;
        com.nereusstream.delay.protocol.TargetOrdinaryPublicationBinding publication = null;
        if (channel != null) {
            final byte[] bindingKey = TargetKeyCodec.scheduleBinding(claimed.locator().scheduleBindingDigest());
            final var binding = TargetScheduleBinding.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.ID, bindingKey), TargetScheduleBinding.VALUE_TYPE)
                    .payload());
            final var physical = CanonicalTargetPartition.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.META, TargetKeyCodec.identity(claimed.locator().target())),
                            CanonicalTargetPartition.VALUE_TYPE)
                    .payload());
            publication = com.nereusstream.delay.protocol.TargetOrdinaryPublicationBinding.fromClaim(
                    claim, claimed, binding, physical, channel, capabilityProfile,
                    com.nereusstream.delay.protocol.ArtifactGenerationSet.current(
                            1, com.nereusstream.delay.protocol.PulsarSourceLock.digest(), bytes(32, 0xC3)).setDigest());
        }
        final var body = new TargetPublishAdmissionBody(
                at.shardId(),
                retryUntil,
                owner,
                claim.storeIncarnation(),
                claim.claimId(),
                claim.work().locator(),
                attemptNo,
                attemptId,
                obligation,
                claim.executionBytes(),
                new CapacityVector(reserved),
                CapacityVector.empty(),
                decisionTime,
                publication,
                publication == null ? null : claim);
        final var author = AuthorIdentity.owner(
                owner.deploymentId(), owner.workerRunId(), owner.ownerEpoch(), owner.leaseFencingDigest());
        final var mutation = SystemMutation.signed(
                at.shardId(),
                SystemMutationType.TARGET_PUBLISH_ADMISSION,
                retryUntil,
                attemptId,
                body.canonicalBytes(),
                author.canonicalBytes(),
                1,
                keys.getPrivate());
        return new TargetAdmissionFixture(new SourceReplayMutation(mutation, at, null, null), attemptId);
    }

    private static void assertTargetAdmissionCommitted(
            ShardStore store, TargetClaimRecord claim, TargetAdmissionFixture admission) {
        final var entry = admission.entry();
        final byte[] messageKey = TargetKeyCodec.message(claim.work().locator().messageId());
        final var admitted = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                        store.get(ColumnFamily.ID, messageKey), TargetMessageRecord.VALUE_TYPE)
                .payload());
        assertEquals(CurrentSendWorkKind.PUBLISHING, admitted.runtime().currentWorkKind());
        assertArrayEquals(admission.attemptId(), admitted.runtime().publishAttemptId());
        assertNull(store.get(ColumnFamily.INFLIGHT, claim.key()));
        assertNull(store.get(ColumnFamily.META, claim.chargeKey()));
        final byte[] attemptBudgetKey = Bytes.concat(
                new byte[] {
                    (byte) TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG,
                    TargetKeyCodec.KEY_FORMAT
                },
                admission.attemptId());
        final var attemptBudget = TargetQuotaAttemptBudget.decode(TargetValueEnvelope.decode(
                        store.get(ColumnFamily.META, attemptBudgetKey), TargetQuotaAttemptBudget.VALUE_TYPE)
                .payload());
        assertEquals(TargetQuotaAttemptBudget.Phase.ADMITTED, attemptBudget.phase());
        assertEquals(claim.work().locator(), attemptBudget.locator());
        if (admitted.locator().orderingMode() == OrderingMode.DELIVERY_TIME_FIFO) {
            final byte[] orderKey =
                    TargetKeyCodec.orderState(admitted.locator().target(), admitted.locator().orderingDomain());
            final var order = TargetOrderState.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.META, orderKey), TargetOrderState.VALUE_TYPE)
                    .payload());
            final var barrier = TargetOrderBarrier.fromMessage(admitted);
            assertEquals(TargetOrderState.OrderingContract.ADMISSION_WATERMARK, order.orderingContract());
            assertArrayEquals(barrier.order().encodedKey(), order.lastAdmittedOrder().encodedKey());
            assertEquals(barrier, order.barrier());
            assertNull(order.serviceableHead());
        }
        final var first = TargetResultRecord.decode(TargetValueEnvelope.decode(
                        store.get(ColumnFamily.DEDUPE, systemKey(entry.mutation())), TargetResultRecord.VALUE_TYPE)
                .payload());
        final var firstResult = SystemMutationResult.decode(first.typedPayload());
        assertEquals(ApplyStatus.APPLIED, firstResult.applyStatus());
        assertEquals(StableCode.OK, firstResult.stableCode());
        final byte[] positionKey = Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT},
                entry.position().canonicalBytes());
        final var position = TargetResultRecord.decode(TargetValueEnvelope.decode(
                        store.get(ColumnFamily.DEDUPE, positionKey), TargetResultRecord.VALUE_TYPE)
                .payload());
        position.requireFirst(first);
    }

    private static void assertUnprovedTypedTargetOutcomeLeavesSourceUnchanged(
            TargetStoreBackend backend,
            ShardStore store,
            TargetQuotaScope scope,
            byte[] lineage,
            SourceReplayMutation entry,
            OwnerIdentity owner,
            SystemMutation admissionImage,
            com.nereusstream.delay.protocol.RetryPolicySemantic retryPolicy,
            KeyPair keys)
            throws java.security.GeneralSecurityException {
        final var original = entry.mutation();
        final var body = PublishOutcomeBody.decode(original.canonicalBody());
        final byte[] policy = new com.nereusstream.delay.protocol.RetryPolicyRef(
                        Bytes.utf8("target-outcome-policy"), 1, bytes(32, 0xA3))
                .canonicalBytes();
        final byte[] typedRetry = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 5);
            CanonicalProtobuf.bytes(output, 2, policy);
            CanonicalProtobuf.uint32(output, 3, 1);
            CanonicalProtobuf.uint64(output, 4, body.observedAt().earliestEpochMs());
            CanonicalProtobuf.uint64(output, 5, original.retryUntilEpochMs());
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, body.stableCode().wireValue());
            CanonicalProtobuf.uint32(output, 9, 1);
        });
        final byte[] encoded = PublishOutcomeBody.encodeInitial(
                scope.shard(),
                original.retryUntilEpochMs(),
                body.publishAttemptId(),
                body.sideEffect(),
                body.disposition(),
                body.stableCode(),
                null,
                body.transfer(),
                body.observedAt(),
                typedRetry);
        final var typedMutation = SystemMutation.signed(
                scope.shard(),
                SystemMutationType.PUBLISH_OUTCOME,
                original.retryUntilEpochMs(),
                body.publishAttemptId(),
                encoded,
                original.authorIdentity(),
                1,
                keys.getPrivate());
        final var outcomes = new TargetPublishOutcomeStore(backend, scope, lineage, 16, 1, 1);
        final long beforeSequence = store.latestSequenceNumber();
        final var beforeSource = store.appliedShardLogPosition();
        assertThrows(
                IllegalStateException.class,
                () -> outcomes.prepareFirst(
                        budget(),
                        typedMutation,
                        entry.position(),
                        (actualScope, writer, mutation, source) -> new TargetPublishOutcomeVerifier.Authorization(
                                keys.getPublic(),
                                ProtocolTuple.currentSystemMutation(),
                                owner,
                                10,
                                10,
                                100,
                                (boundScope, boundWriter, position, evidence) -> true)));
        final var changedAdmission = SystemMutation.signed(
                admissionImage.shardId(),
                admissionImage.type(),
                admissionImage.retryUntilEpochMs(),
                admissionImage.logicalOperationIdentity(),
                admissionImage.canonicalBody(),
                admissionImage.authorIdentity(),
                2,
                keys.getPrivate());
        assertThrows(
                IllegalStateException.class,
                () -> outcomes.prepareFirst(
                        budget(),
                        original,
                        entry.position(),
                        (actualScope, writer, mutation, source) -> new TargetPublishOutcomeVerifier.Authorization(
                                keys.getPublic(),
                                ProtocolTuple.currentSystemMutation(),
                                owner,
                                10,
                                10,
                                100,
                                (boundScope, boundWriter, position, evidence) -> true,
                                new TargetPublishOutcomeVerifier.RetryContext(
                                        changedAdmission, admissionImage, retryPolicy))));
        assertEquals(beforeSequence, store.latestSequenceNumber());
        assertEquals(beforeSource, store.appliedShardLogPosition());
        assertNull(store.get(ColumnFamily.DEDUPE, systemKey(typedMutation)));
    }

    private static void assertExhaustedRetryDoesNotWrite(
            TargetStoreBackend backend,
            ShardStore store,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetAdmissionFixture first,
            TargetAdmissionFixture current,
            OwnerIdentity owner,
            com.nereusstream.delay.protocol.RetryPolicySemantic policy,
            KeyPair keys) {
        final var currentBody = TargetPublishAdmissionBody.decode(current.entry().mutation().canonicalBody());
        final var firstBody = TargetPublishAdmissionBody.decode(first.entry().mutation().canonicalBody());
        final var message = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                        store.get(ColumnFamily.ID, TargetKeyCodec.message(currentBody.locator().messageId())),
                        TargetMessageRecord.VALUE_TYPE)
                .payload());
        final var beforeSource = (KafkaSourcePosition) store.appliedShardLogPosition();
        final var at = source(beforeSource, beforeSource.offset() + 1, beforeSource.brokerPersistenceTimeEpochMs() + 1);
        final var observed = new TrustedUtcIntervalEvidence(
                at.brokerPersistenceTimeEpochMs(), at.brokerPersistenceTimeEpochMs() + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                bytes(32, 0xA1), 1, 1, 1, bytes(32, 0xA2), 0, null);
        final long next = observed.latestEpochMs() + com.nereusstream.delay.protocol.RetryJitter.delayMs(
                com.nereusstream.delay.protocol.RetryJitter.MESSAGE_PUBLISH,
                message.locator().messageId(), Integer.toUnsignedLong(message.locator().generation()),
                Integer.toUnsignedLong(currentBody.attemptNo()), policy.retryBackoffCap(currentBody.attemptNo()));
        final long retryUntil = at.brokerPersistenceTimeEpochMs() + 10_000;
        final byte[] placeholder = CanonicalProtobuf.message(
                output -> CanonicalProtobuf.bytes(output, 1, Bytes.utf8("unknown")));
        final byte[] body = PublishOutcomeBody.encodeInitial(
                scope.shard(), retryUntil, currentBody.publishAttemptId(), 3, 4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, null, placeholder, observed,
                typedUnknownRetryDecision(
                        policy, firstBody.decisionTime().latestEpochMs(),
                        Math.min(message.expireAtEpochMs(),
                                firstBody.decisionTime().latestEpochMs() + policy.maxRetryDurationMs()),
                        currentBody.attemptNo(), next));
        final var mutation = SystemMutation.signed(
                scope.shard(), SystemMutationType.PUBLISH_OUTCOME, retryUntil, currentBody.publishAttemptId(), body,
                current.entry().mutation().authorIdentity(), 1, keys.getPrivate());
        final long beforeSequence = store.latestSequenceNumber();
        final var outcomes = new TargetPublishOutcomeStore(backend, scope, lineage, 16, 1, 1);
        final var exhausted = assertThrows(IllegalArgumentException.class, () -> outcomes.prepareFirst(
                budget(), mutation, at,
                (a, b, c, d) -> new TargetPublishOutcomeVerifier.Authorization(
                        keys.getPublic(), ProtocolTuple.currentSystemMutation(), owner, 10, 10, 100,
                        (e, f, g, h) -> true,
                        new TargetPublishOutcomeVerifier.RetryContext(
                                current.entry().mutation(), first.entry().mutation(), policy))));
        assertTrue(exhausted.getMessage().contains("policy/jitter/budget"));
        assertEquals(beforeSequence, store.latestSequenceNumber());
        assertEquals(beforeSource, store.appliedShardLogPosition());
        assertNull(store.get(ColumnFamily.DEDUPE, systemKey(mutation)));
    }

    private static void assertRecoveredTargetOutcome(
            ShardStore store, TargetQuotaScope scope, TargetWorkerShardRuntime oldWorker,
            OwnerLease oldLease, SourceAssignment assignment, OxiaOwnerLeaseStore releasingLeases,
            OxiaOwnerLeaseStore leases, byte[] session,
            com.nereusstream.delay.store.SharedRocksDbResources resources, WorkClassExecutionRegistry classes,
            TargetAdmissionFixture admission, OwnerIdentity oldOwner, KeyPair keys,
            com.nereusstream.delay.protocol.RetryPolicySemantic policy, boolean loseOwnerDuringLoad) {
        final var admissionImage = admission.entry().mutation();
        final var admitted = TargetPublishAdmissionBody.decode(admissionImage.canonicalBody());
        final byte[] messageKey = TargetKeyCodec.message(admitted.locator().messageId());
        final byte[] budgetKey = Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG, TargetKeyCodec.KEY_FORMAT},
                admitted.publishAttemptId());
        final var originalBudget = TargetQuotaAttemptBudget.decode(TargetValueEnvelope.decode(
                store.get(ColumnFamily.META, budgetKey), TargetQuotaAttemptBudget.VALUE_TYPE).payload());
        oldWorker.pauseNewTurns();
        oldWorker.closeSource();
        assertTrue(releasingLeases.release(oldLease));
        final var acquiring = leases.acquire(assignment, "target-publish-recovery", session, 102, 10_000)
                .orElseThrow();
        assertTrue(Long.compareUnsigned(acquiring.ownerEpoch(), oldLease.ownerEpoch()) > 0);
        assertFalse(releasingLeases.release(oldLease));
        final TargetStoreBackend.ReadAuthority reads = (metadata, actualScope) -> new TargetStoreBackend.CommitGuard() {
            @Override
            public void requireCurrent() {
                assertEquals(scope, actualScope);
                assertArrayEquals(store.metadata().encode(), metadata.encode());
                assertTrue(acquiring.sameIdentity(leases.current(scope.shard()).orElseThrow()));
            }
            @Override
            public void close() {}
        };
        final var reopened = TargetStoreBootstrap.reopen(store, scope,
                new TargetStoreBackend.WriteLimits(64, 2 << 20), budget(), reads);
        final var active = TargetWorkerOwnerActivation.activate(
                reopened, store, assignment, acquiring, leases, () -> 102);
        final var owner = new OwnerIdentity(oldOwner.deploymentId(), bytes(16, 0xF2),
                active.ownerEpoch(), active.leaseToken());
        final var resolutions = new java.util.concurrent.atomic.AtomicInteger();
        final var runtime = new TargetSourceApplyRuntime(reopened, store, assignment, active,
                new TargetSourceApplyRuntime.Authorities(leases, SourceReplaySuccessor.strictKafka(),
                        e -> { throw new AssertionError("unexpected recovery grant"); },
                        e -> { throw new AssertionError("unexpected recovery fence"); },
                        e -> { throw new AssertionError("unexpected recovery expiry"); },
                        e -> { throw new AssertionError("unexpected recovery Close"); },
                        e -> { throw new AssertionError("unexpected recovery membership"); },
                        (a, b, c) -> guard(), reads,
                        e -> { throw new AssertionError("unexpected recovery command"); },
                        e -> { throw new AssertionError("unexpected recovery Native control"); },
                        e -> { throw new AssertionError("recovery must not create another Admission"); },
                        e -> new TargetSourceApplyRuntime.OutcomeControl((actualScope, writer, mutation, at) -> {
                            resolutions.incrementAndGet();
                            assertEquals(scope, actualScope);
                            assertEquals(owner, writer.asOwnerIdentity());
                            return new TargetPublishOutcomeVerifier.Authorization(keys.getPublic(),
                                    ProtocolTuple.currentSystemMutation(), owner, 10, 10, 100,
                                    (a, b, c, d) -> true, new TargetPublishOutcomeVerifier.RetryContext(
                                            admissionImage, admissionImage, policy));
                        }, (a, b, c) -> guard())),
                new TargetSourceApplyRuntime.Limits(4096, 32L << 20, 60_000_000_000L, 16, 1), System::nanoTime);
        final var pending = new java.util.concurrent.atomic.AtomicReference<SourceRecordConsumer.PolledSourceRecord>();
        final var worker = TargetWorkerShardFactory.create(
                () -> java.util.Optional.ofNullable(pending.getAndSet(null)), runtime.acceptedAssignment(),
                classes, store, resources, runtime,
                new TargetWorkerShardRuntime.Maintenance(
                        new TargetCloseStore(reopened.backend(), scope, reopened.root().recoveryLineage(), 16, 1)
                                .reservationControls((a, b) -> java.util.Optional.empty()),
                        new TargetReservationClosureWorkClassExecutor.Limits(4096, 250_000, 60_000_000_000L),
                        new TargetReservationExpiryWorkClassExecutor.Limits(2048, 100_000, 60_000_000_000L),
                        (a, b, c) -> guard(), ignored -> {}, ignored -> {}, ignored -> {}, () -> 102));
        assertThrows(IllegalStateException.class,
                () -> oldWorker.readRecoveryAdmission(budget(), admissionImage, () -> 102));
        assertThrows(IllegalStateException.class,
                () -> worker.readAppliedAdmission(budget(), admissionImage, () -> 102));
        final long beforeDiscovery = store.latestSequenceNumber();
        final var discoveryBudget = budget();
        final var discovered = worker.discoverPublishRecovery(discoveryBudget, null, 1, () -> 102);
        assertEquals(TargetPublishRecoveryDiscovery.Stop.PAGE_LIMIT, discovered.stop());
        assertFalse(discovered.complete());
        assertEquals(1, discovered.entries().size());
        final var reference = discovered.entries().getFirst();
        assertEquals(oldOwner, reference.admittedOwner());
        assertEquals(admission.entry().position(), reference.source());
        assertArrayEquals(Bytes.sha256(admissionImage.canonicalEnvelope()), reference.envelopeDigest());
        assertEquals(admissionImage, reference.requireImage(admissionImage, admission.entry().position()));
        final var changedImage = SystemMutation.signed(admissionImage.shardId(), admissionImage.type(),
                admissionImage.retryUntilEpochMs(), admissionImage.logicalOperationIdentity(),
                admissionImage.canonicalBody(), admissionImage.authorIdentity(), 2, keys.getPrivate());
        assertThrows(IllegalStateException.class,
                () -> reference.requireImage(changedImage, admission.entry().position()));
        final var originalPosition = (KafkaSourcePosition) admission.entry().position();
        assertThrows(IllegalStateException.class, () -> reference.requireImage(admissionImage,
                source(originalPosition, originalPosition.offset() + 1,
                        originalPosition.brokerPersistenceTimeEpochMs())));
        final var rangeEnd = worker.discoverPublishRecovery(budget(), discovered.continuation(), 1, () -> 102);
        assertTrue(rangeEnd.complete());
        assertTrue(rangeEnd.entries().isEmpty());
        final var shortBudget = new BoundedReadBudget(Math.toIntExact(discoveryBudget.actualRecords() - 1),
                32L << 20, 60_000_000_000L, System::nanoTime);
        final var incomplete = worker.discoverPublishRecovery(shortBudget, null, 1, () -> 102);
        assertEquals(TargetPublishRecoveryDiscovery.Stop.READ_BUDGET, incomplete.stop());
        assertFalse(incomplete.complete());
        assertTrue(incomplete.entries().isEmpty());
        final var resumed = worker.discoverPublishRecovery(budget(), incomplete.continuation(), 1, () -> 102);
        assertEquals(1, resumed.entries().size());
        final var foreign = new TargetPublishRecoveryDiscovery(
                reopened.backend(), scope, reopened.root().recoveryLineage());
        assertThrows(IllegalArgumentException.class,
                () -> foreign.scan(budget(), discovered.continuation(), 1, reads));
        assertEquals(beforeDiscovery, store.latestSequenceNumber());
        final byte[] firstProofKey = systemKey(admissionImage);
        final byte[] firstProof = store.get(ColumnFamily.DEDUPE, firstProofKey);
        store.write(batch -> batch.delete(ColumnFamily.DEDUPE, firstProofKey));
        final long missingProofVersion = store.latestSequenceNumber();
        assertThrows(IllegalStateException.class,
                () -> worker.discoverPublishRecovery(budget(), null, 1, () -> 102));
        assertEquals(missingProofVersion, store.latestSequenceNumber());
        store.write(batch -> batch.put(ColumnFamily.DEDUPE, firstProofKey, firstProof));
        final var restoredReference = worker.discoverPublishRecovery(budget(), null, 1, () -> 102).entries().getFirst();
        assertEquals(admissionImage, restoredReference.requireImage(admissionImage, admission.entry().position()));
        final var before = (KafkaSourcePosition) store.appliedShardLogPosition();
        final var at = source(before, before.offset() + 1, before.brokerPersistenceTimeEpochMs() + 1);
        final long observed = Math.max(at.brokerPersistenceTimeEpochMs(), admitted.decisionTime().latestEpochMs());
        final var time = new TrustedUtcIntervalEvidence(observed, observed + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                bytes(32, 0xF3), 1, 1, 1, bytes(32, 0xF4), 0, null);
        final byte[] retry = typedUnknownRetryDecision(policy, admitted.decisionTime().latestEpochMs(),
                Math.min(admitted.publication().expireAtEpochMs(),
                        admitted.decisionTime().latestEpochMs() + policy.maxRetryDurationMs()),
                admitted.attemptNo(), null);
        final byte[] zero = new PublishAdmissionBody.ChargeVector(
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0).canonicalBytes();
        final var appends = new java.util.concurrent.atomic.AtomicInteger();
        final var handoff = new com.nereusstream.delay.ownership.TargetOutcomeWorkClassExecutor(worker, image -> {
            appends.incrementAndGet();
            return com.nereusstream.delay.ownership.ShardLogMutationAppender.AppendOutcome.persisted(at);
        });
        final var factory = new com.nereusstream.delay.ownership.TargetPublishOutcomeMutationFactory(
                1, keys.getPrivate());
        assertThrows(IllegalArgumentException.class,
                () -> new com.nereusstream.delay.ownership.TargetPublishRecoveryExecutor(worker,
                        new com.nereusstream.delay.ownership.TargetOutcomeWorkClassExecutor(oldWorker,
                                image -> { throw new AssertionError("wrong Worker must not append"); }), factory));
        final var recovery = new com.nereusstream.delay.ownership.TargetPublishRecoveryExecutor(
                worker, handoff, factory);
        final var context = new com.nereusstream.delay.ownership.WorkerPublishOutcomeMutationFactory.OutcomeContext(
                time.latestEpochMs() + 10_000, 4, zero, time, retry);
        final var loads = new java.util.concurrent.atomic.AtomicInteger();
        final var firstHistory = new java.util.concurrent.CompletableFuture<
                com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Prepared>();
        final var secondHistory = new java.util.concurrent.CompletableFuture<
                com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Prepared>();
        final var host = TargetWorkerHostTestBridge.withoutMaintenanceTimer(classes, resources, List.of(worker));
        final var maintenance = host.configurePublishRecoveryMaintenance(worker, factory,
                image -> {
                    appends.incrementAndGet();
                    return com.nereusstream.delay.ownership.ShardLogMutationAppender.AppendOutcome.persisted(at);
                }, TargetCommandStoreTest::budget,
                ref -> {
                    assertEquals(admissionImage, ref.requireImage(admissionImage, admission.entry().position()));
                    return loads.incrementAndGet() == 1 ? firstHistory : secondHistory;
                }, () -> 102);
        final var competingLoadCount = new java.util.concurrent.atomic.AtomicInteger();
        final var competingHistory = new java.util.concurrent.CompletableFuture<
                com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Prepared>();
        final var competing = new com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance(worker,
                recovery, TargetCommandStoreTest::budget, ref -> {
                    competingLoadCount.incrementAndGet();
                    return competingHistory;
                }, () -> 102);
        assertEquals(com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Status.DISCOVERED,
                competing.runTurn().status());
        assertEquals(com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Status.LOADING,
                competing.runTurn().status());
        assertThrows(IllegalStateException.class, () -> host.configurePublishRecoveryMaintenance(worker, factory,
                image -> { throw new AssertionError("duplicate configuration must not append"); },
                TargetCommandStoreTest::budget, ref -> firstHistory, () -> 102));
        assertEquals(com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Status.DISCOVERED,
                host.runNextPublishRecoveryTurn().orElseThrow().turn().orElseThrow().status());
        assertEquals(com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Status.LOADING,
                host.runNextPublishRecoveryTurn().orElseThrow().turn().orElseThrow().status());
        assertEquals(com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Status.LOADING,
                host.runNextPublishRecoveryTurn().orElseThrow().turn().orElseThrow().status());
        assertEquals(1, loads.get());
        assertEquals(0, appends.get());
        firstHistory.completeExceptionally(
                new IllegalStateException("fixture protected history temporarily unavailable"));
        final var loadFailed = host.runNextPublishRecoveryTurn().orElseThrow().turn().orElseThrow();
        assertEquals(com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Status.FAILED,
                loadFailed.status());
        assertEquals("fixture protected history temporarily unavailable", loadFailed.failure().getMessage());
        assertEquals(com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Status.LOADING,
                host.runNextPublishRecoveryTurn().orElseThrow().turn().orElseThrow().status());
        if (loseOwnerDuringLoad) {
            final long beforeLoss = store.latestSequenceNumber();
            assertTrue(leases.release(active));
            secondHistory.complete(new com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Prepared(
                    admissionImage, owner, context));
            final var lost = host.runNextPublishRecoveryTurn().orElseThrow().turn().orElseThrow();
            assertEquals(com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Status.FAILED,
                    lost.status());
            assertNotNull(lost.failure());
            assertEquals(0, appends.get());
            assertEquals(2, loads.get());
            assertEquals(beforeLoss, store.latestSequenceNumber());
            assertTrue(maintenance.pendingMutation().isEmpty());
            final var retained = TargetQuotaAttemptBudget.decode(TargetValueEnvelope.decode(
                    store.get(ColumnFamily.META, budgetKey), TargetQuotaAttemptBudget.VALUE_TYPE).payload());
            assertEquals(TargetQuotaAttemptBudget.Phase.ADMITTED, retained.phase());
            worker.pauseNewTurns();
            worker.closeSource();
            return;
        }
        secondHistory.complete(new com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Prepared(
                admissionImage, owner, context));
        final var submitted = host.runNextPublishRecoveryTurn().orElseThrow().turn().orElseThrow();
        assertEquals(com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Status.SUBMITTED,
                submitted.status(), () -> String.valueOf(submitted.failure()));
        assertEquals(owner, AuthorIdentity.decode(submitted.mutation().authorIdentity()).asOwnerIdentity());
        assertEquals(com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Status.WAITING_SOURCE,
                host.runNextPublishRecoveryTurn().orElseThrow().turn().orElseThrow().status());
        final byte[] exact = submitted.mutation().encodeFrame();
        classes.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
        assertEquals(1, appends.get());
        assertArrayEquals(exact, maintenance.pendingMutation().orElseThrow().encodeFrame());
        final var entry = new SourceReplayMutation(submitted.mutation(), at, null, null);
        final var acks = new java.util.concurrent.atomic.AtomicInteger();
        pending.set(new SourceRecordConsumer.PolledSourceRecord(entry, (actual, result) -> {
            assertEquals(ApplyStatus.APPLIED, result.systemMutationResult().applyStatus());
            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, result.systemMutationResult().stableCode());
            return acks.incrementAndGet() == 1 ? SourceAcknowledgement.AcknowledgementResult.unknown(null)
                    : SourceAcknowledgement.AcknowledgementResult.acked();
        }));
        final var turn = worker.runSourceTurn(new SchedulerBudget(64, 32L << 20, 60_000_000_000L), () -> 102);
        assertEquals(SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN, turn.status(),
                () -> String.valueOf(turn.failure()));
        final long applied = store.latestSequenceNumber();
        final var after = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                store.get(ColumnFamily.ID, messageKey), TargetMessageRecord.VALUE_TYPE).payload());
        assertEquals(GenerationAggregateState.UNCERTAIN, after.aggregateState());
        assertEquals(CurrentSendWorkKind.NONE, after.runtime().currentWorkKind());
        assertEquals(oldOwner.ownerEpoch(), after.runtime().attemptObligations().getFirst().ownerEpoch());
        final var unknown = TargetQuotaAttemptBudget.decode(TargetValueEnvelope.decode(
                store.get(ColumnFamily.META, budgetKey), TargetQuotaAttemptBudget.VALUE_TYPE).payload());
        assertEquals(TargetQuotaAttemptBudget.Phase.UNKNOWN, unknown.phase());
        assertEquals(originalBudget.commitment(), unknown.commitment());
        assertEquals(originalBudget.allocated(), unknown.allocated());
        assertEquals(1, unknown.effectiveCharge().amount(CapacityDimension.INFLIGHT_MESSAGES));
        final var order = TargetOrderState.decode(TargetValueEnvelope.decode(store.get(ColumnFamily.META,
                TargetKeyCodec.orderState(after.locator().target(), after.locator().orderingDomain())),
                TargetOrderState.VALUE_TYPE).payload());
        order.requireBarrierProjection(after);
        assertNull(order.serviceableHead());
        assertEquals(SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED,
                worker.runSourceTurn(new SchedulerBudget(64, 32L << 20, 60_000_000_000L), () -> 102).status());
        assertEquals(2, acks.get());
        assertEquals(1, resolutions.get());
        assertEquals(applied, store.latestSequenceNumber());
        final var settled = host.runNextPublishRecoveryTurn().orElseThrow().turn().orElseThrow();
        assertEquals(com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Status.SETTLED,
                settled.status());
        assertEquals(1, appends.get());
        assertEquals(2, loads.get());
        assertArrayEquals(exact, settled.mutation().encodeFrame());
        assertTrue(maintenance.pendingMutation().isEmpty());
        assertEquals(com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Status.LOADING,
                competing.runTurn().status());
        assertEquals(1, competingLoadCount.get());
        competingHistory.complete(new com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Prepared(
                admissionImage, owner, context));
        assertEquals(com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Status.ALREADY_HANDLED,
                competing.runTurn().status());
        assertEquals(1, appends.get());
        assertTrue(competing.pendingMutation().isEmpty());
        assertThrows(IllegalStateException.class,
                () -> worker.discoverPublishRecovery(budget(), discovered.continuation(), 1, () -> 102));
        final var settledDiscovery = worker.discoverPublishRecovery(budget(), null, 2, () -> 102);
        assertTrue(settledDiscovery.complete());
        assertTrue(settledDiscovery.entries().isEmpty());
        assertEquals(applied, store.latestSequenceNumber());
        assertEquals(com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Status.SCAN_YIELD,
                host.runNextPublishRecoveryTurn().orElseThrow().turn().orElseThrow().status());
        store.write(batch -> batch.put(ColumnFamily.DEDUPE, firstProofKey, firstProof));
        assertEquals(com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Status.RESTART_SCAN,
                host.runNextPublishRecoveryTurn().orElseThrow().turn().orElseThrow().status());
        assertEquals(com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Status.SCAN_YIELD,
                host.runNextPublishRecoveryTurn().orElseThrow().turn().orElseThrow().status());
        assertEquals(com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Status.IDLE,
                host.runNextPublishRecoveryTurn().orElseThrow().turn().orElseThrow().status());
        assertEquals(2, loads.get());
        assertEquals(1, appends.get());
        worker.pauseNewTurns();
        worker.closeSource();
        assertTrue(leases.release(active));
    }

    private static void assertRecoveryOutcomeSnapshot(
            TargetWorkerShardRuntime worker, ShardStore store, SystemMutation admission,
            OwnerIdentity owner, KeyPair keys, com.nereusstream.delay.protocol.RetryPolicySemantic policy,
            long until, TrustedUtcIntervalEvidence observed, byte[] retry, byte[] transfer, SystemMutation expected) {
        final long before = store.latestSequenceNumber();
        final var recovery = worker.readRecoveryAdmission(budget(), admission, () -> 101);
        assertArrayEquals(admission.canonicalEnvelope(), recovery.image().canonicalEnvelope());
        final var factory = new com.nereusstream.delay.ownership.TargetPublishOutcomeMutationFactory(
                1, keys.getPrivate());
        final var context = new com.nereusstream.delay.ownership.WorkerPublishOutcomeMutationFactory.OutcomeContext(
                until, 4, transfer, observed, retry);
        assertArrayEquals(expected.encodeFrame(),
                factory.createRecoveryUnknown(recovery, owner, context).encodeFrame());
        final var successor = new OwnerIdentity(owner.deploymentId(), bytes(16, 0xE6),
                owner.ownerEpoch() + 1, bytes(32, 0xE7));
        final var signed = factory.createRecoveryUnknown(recovery, successor, context);
        assertEquals(successor, AuthorIdentity.decode(signed.authorIdentity()).asOwnerIdentity());
        assertTrue(signed.verifySignature(keys.getPublic()));
        final var body = PublishOutcomeBody.decode(signed.canonicalBody());
        assertEquals(3, body.sideEffect());
        assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, body.stableCode());
        assertEquals(5, body.retryDecision().kind());
        assertFalse(body.retryDecision().hasNextRetryAt());
        final var source = new com.nereusstream.delay.ownership.TargetOutcomeWorkClassExecutor(worker,
                mutation -> { throw new AssertionError("unowned recovery must not append"); });
        assertThrows(IllegalStateException.class, () -> source.submit(signed, () -> 101));
        assertThrows(IllegalArgumentException.class, () -> factory.createRecoveryUnknown(recovery, owner,
                new com.nereusstream.delay.ownership.WorkerPublishOutcomeMutationFactory.OutcomeContext(
                        until, 0, transfer, observed, retry)));
        final byte[] scheduled = typedUnknownRetryDecision(policy, body.retryDecision().firstAttemptAt(),
                body.retryDecision().retryDeadline(), recovery.body().attemptNo(),
                body.retryDecision().firstAttemptAt() + 1);
        assertThrows(IllegalArgumentException.class, () -> factory.createRecoveryUnknown(recovery, owner,
                new com.nereusstream.delay.ownership.WorkerPublishOutcomeMutationFactory.OutcomeContext(
                        until, 4, transfer, observed, scheduled)));
        assertThrows(IllegalArgumentException.class, () -> factory.createRecoveryUnknown(recovery, owner,
                new com.nereusstream.delay.ownership.WorkerPublishOutcomeMutationFactory.OutcomeContext(
                        until, 4, new PublishAdmissionBody.ChargeVector(
                                1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0).canonicalBytes(), observed, retry)));
        assertEquals(before, store.latestSequenceNumber());
    }

    private static TargetPublishOutcomeVerifier.EvidenceContext publishedEvidenceContext(
            SystemMutation admission, com.nereusstream.delay.protocol.PublishEvidence expected) {
        final var publication = TargetPublishAdmissionBody.decode(admission.canonicalBody()).publication();
        return new TargetPublishOutcomeVerifier.EvidenceContext(admission, (actual, evidence, first, at) -> {
            assertEquals(publication, actual);
            assertArrayEquals(expected.canonicalBytes(), evidence.canonicalBytes());
            assertTrue(first.compareTo(at) < 0);
        });
    }

    /** Source/accounting regression; terminal/replay projection and Broker evidence providers remain fixtures. */
    private static void assertLateTargetOutcome(
            TargetStoreBackend backend, ShardStore store, TargetQuotaScope scope, byte[] lineage,
            TargetWorkerShardRuntime worker,
            java.util.concurrent.atomic.AtomicReference<SourceRecordConsumer.PolledSourceRecord> pending,
            TargetAdmissionFixture admission, KeyPair keys, OwnerIdentity owner,
            java.util.concurrent.atomic.AtomicReference<com.nereusstream.delay.protocol.PublishEvidence> expected,
            java.util.concurrent.atomic.AtomicInteger authorityResolutions, int mode) {
        final var body = TargetPublishAdmissionBody.decode(admission.entry().mutation().canonicalBody());
        final boolean historical = mode >= 15;
        final int effect = (mode - 12) % 3 + 1;
        terminalOutcomeFixture(backend, store, scope, lineage, body.locator(), historical);
        final byte[] messageKey = TargetKeyCodec.message(body.locator().messageId());
        final byte[] currentBytes = store.get(ColumnFamily.ID, messageKey);
        final var current = TargetMessageRecord.decode(
                TargetValueEnvelope.decode(currentBytes, TargetMessageRecord.VALUE_TYPE).payload());
        final byte[] terminalKey = TargetTerminalGenerationRecord.key(body.locator());
        final var terminal = TargetTerminalGenerationRecord.decode(TargetValueEnvelope.decode(
                store.get(ColumnFamily.TERMINAL, terminalKey), TargetTerminalGenerationRecord.VALUE_TYPE).payload());
        final byte[] orderKey = TargetKeyCodec.orderState(body.locator().target(), body.locator().orderingDomain());
        final byte[] orderBytes = store.get(ColumnFamily.META, orderKey);
        final var order = TargetOrderState.decode(TargetValueEnvelope.decode(
                orderBytes, TargetOrderState.VALUE_TYPE).payload());
        if (historical) {
            order.requireTerminalBarrierProjection(terminal);
            assertNull(order.serviceableHead());
        }
        final byte[] payloadKey = Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, TargetKeyCodec.KEY_FORMAT},
                body.locator().messageId().bytes());
        final byte[] payloadBytes = store.get(ColumnFamily.META, payloadKey);
        final var priorSource = (KafkaSourcePosition) store.appliedShardLogPosition();
        final var at = source(priorSource, priorSource.offset() + 1, priorSource.brokerPersistenceTimeEpochMs() + 1);
        final long observed = Math.max(at.brokerPersistenceTimeEpochMs(), body.decisionTime().latestEpochMs());
        final var time = new TrustedUtcIntervalEvidence(observed, observed + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, bytes(32, 0xA1),
                1, 1, 1, bytes(32, 0xA2), 0, null);
        final StableCode cause = effect == 1 ? StableCode.OK
                : effect == 2 ? StableCode.DESTINATION_DEFINITIVE_PERMANENT : StableCode.RECOVERY_FIRST_SEND_UNCERTAIN;
        final var evidence = effect == 1 ? targetPublishedAck(
                body.publication(), body.publication().preparedPublishHash())
                : effect == 2 ? targetRejectedEvidence(body.publication(), body.publication().preparedPublishHash(),
                        body.publication().physical().physicalPartition()) : null;
        expected.set(evidence);
        final var policy = targetOutcomeRetryPolicy(false);
        final byte[] retry = com.nereusstream.delay.protocol.CanonicalProtobuf.message(out -> {
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(out, 1, effect == 1 ? 1 : effect == 2 ? 3 : 5);
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 2, policy.ref().canonicalBytes());
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(out, 3, body.attemptNo());
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint64(out, 4, body.decisionTime().latestEpochMs());
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint64(out, 5, Math.min(
                    body.publication().expireAtEpochMs(),
                    body.decisionTime().latestEpochMs() + policy.maxRetryDurationMs()));
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(out, 7, 1);
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(out, 8, cause.wireValue());
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(out, 9, 1);
        });
        final byte[] transfer = effect == 3 ? new PublishAdmissionBody.ChargeVector(
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0).canonicalBytes() : body.outcomeTransfer();
        final var mutation = SystemMutation.signed(scope.shard(), SystemMutationType.PUBLISH_OUTCOME, observed + 10_000,
                body.publishAttemptId(), PublishOutcomeBody.encodeInitial(scope.shard(), observed + 10_000,
                        body.publishAttemptId(), effect, effect == 1 ? 0 : effect == 2 ? 2 : 4, cause,
                        evidence == null ? new byte[0] : evidence.canonicalBytes(), transfer, time, retry),
                AuthorIdentity.owner(owner.deploymentId(), owner.workerRunId(), owner.ownerEpoch(),
                        owner.leaseFencingDigest()).canonicalBytes(), 1, keys.getPrivate());
        final var outcomes = new TargetPublishOutcomeStore(backend, scope, lineage, 16, 1, 1);
        final byte[] terminalBytes = store.get(ColumnFamily.TERMINAL, terminalKey);
        store.write(batch -> batch.delete(ColumnFamily.TERMINAL, terminalKey));
        final long missingSequence = store.latestSequenceNumber();
        assertThrows(IllegalStateException.class, () -> outcomes.prepareFirst(budget(), mutation, at,
                (a, b, c, d) -> new TargetPublishOutcomeVerifier.Authorization(keys.getPublic(),
                        ProtocolTuple.currentSystemMutation(), owner, 10, 10, 100, (e, f, g, h) -> true,
                        new TargetPublishOutcomeVerifier.RetryContext(admission.entry().mutation(),
                                admission.entry().mutation(), policy),
                        effect == 3 ? null : publishedEvidenceContext(admission.entry().mutation(), evidence))));
        assertEquals(missingSequence, store.latestSequenceNumber());
        assertEquals(priorSource, store.appliedShardLogPosition());
        store.write(batch -> batch.put(ColumnFamily.TERMINAL, terminalKey, terminalBytes));
        final var entry = new SourceReplayMutation(mutation, at, null, null);
        final var acknowledgements = new java.util.concurrent.atomic.AtomicInteger();
        pending.set(new SourceRecordConsumer.PolledSourceRecord(entry, (actual, result) -> {
            assertEquals(entry, actual);
            assertEquals(ApplyStatus.APPLIED, result.systemMutationResult().applyStatus());
            assertEquals(cause, result.systemMutationResult().stableCode());
            return acknowledgements.incrementAndGet() == 1
                    ? SourceAcknowledgement.AcknowledgementResult.unknown(null)
                    : SourceAcknowledgement.AcknowledgementResult.acked();
        }));
        assertEquals(SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN,
                worker.runSourceTurn(new SchedulerBudget(64, 32L << 20, 60_000_000_000L), () -> 101).status());
        assertEquals(at, store.appliedShardLogPosition());
        final var next = TargetTerminalGenerationRecord.decode(TargetValueEnvelope.decode(
                store.get(ColumnFamily.TERMINAL, terminalKey), TargetTerminalGenerationRecord.VALUE_TYPE).payload());
        assertEquals(terminal.terminalCode(), next.terminalCode());
        assertEquals(terminal.runtime().aggregateState(), next.runtime().aggregateState());
        assertEquals(terminal.mutation(), next.mutation());
        assertEquals(terminal.runtime().admissionsUsed(), next.runtime().admissionsUsed());
        assertEquals(effect == 1, next.runtime().possibleDestinationDuplicate());
        assertEquals(terminal.runtime().runtimeRevision() + 1, next.runtime().runtimeRevision());
        assertEquals(effect == 3 ? List.of(body.obligation().uncertain()) : List.of(),
                next.runtime().attemptObligations());
        if (historical) {
            assertArrayEquals(currentBytes, store.get(ColumnFamily.ID, messageKey));
            final var nextOrder = TargetOrderState.decode(TargetValueEnvelope.decode(
                    store.get(ColumnFamily.META, orderKey), TargetOrderState.VALUE_TYPE).payload());
            assertArrayEquals(order.lastAdmittedOrder().encodedKey(), nextOrder.lastAdmittedOrder().encodedKey());
            if (effect == 3) {
                nextOrder.requireTerminalBarrierProjection(next);
                assertNull(nextOrder.serviceableHead());
            } else {
                assertNull(nextOrder.barrier());
                assertEquals(current.locator().messageId(), nextOrder.serviceableHead().messageId());
                assertEquals(current.locator().generation(), nextOrder.serviceableHead().generation());
            }
            assertArrayEquals(payloadBytes, store.get(ColumnFamily.META, payloadKey));
            assertEquals(terminal.stateVersion(), next.stateVersion());
        } else {
            final var after = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                    store.get(ColumnFamily.ID, messageKey), TargetMessageRecord.VALUE_TYPE).payload());
            assertEquals(current.stateVersion() + 1, after.stateVersion());
            assertEquals(after.runtime(), next.runtime());
            assertEquals(after.stateVersion(), next.stateVersion());
            final var nextOrder = TargetOrderState.decode(TargetValueEnvelope.decode(
                    store.get(ColumnFamily.META, orderKey), TargetOrderState.VALUE_TYPE).payload());
            assertArrayEquals(order.lastAdmittedOrder().encodedKey(), nextOrder.lastAdmittedOrder().encodedKey());
            if (effect == 3) {
                nextOrder.requireBarrierProjection(after);
            } else {
                assertNull(nextOrder.barrier());
                // This harness has already canceled its sole follower before the Outcome.
                assertNull(nextOrder.serviceableHead());
            }
            final var payload = TargetQuotaPayloadOwner.decode(TargetValueEnvelope.decode(
                    store.get(ColumnFamily.META, payloadKey), TargetQuotaPayloadOwner.VALUE_TYPE).payload());
            assertEquals(effect == 3 ? TargetQuotaPayloadOwner.Phase.ACTIVE : TargetQuotaPayloadOwner.Phase.RETAINED,
                    payload.phase());
        }
        final byte[] budgetKey = Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG, TargetKeyCodec.KEY_FORMAT},
                body.publishAttemptId());
        final var budget = TargetQuotaAttemptBudget.decode(TargetValueEnvelope.decode(
                store.get(ColumnFamily.META, budgetKey), TargetQuotaAttemptBudget.VALUE_TYPE).payload());
        assertEquals(effect == 3 ? TargetQuotaAttemptBudget.Phase.UNKNOWN
                : TargetQuotaAttemptBudget.Phase.RESOLVED_AWAITING_FLOOR, budget.phase());
        assertEquals(body.commitment(), budget.commitment());
        assertEquals(body.allocated(), budget.allocated());
        assertNull(budget.floorDigest());
        assertEquals(effect == 3 ? 1 : 0, budget.effectiveCharge().amount(CapacityDimension.INFLIGHT_MESSAGES));
        final long applied = store.latestSequenceNumber();
        assertEquals(SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED,
                worker.runSourceTurn(new SchedulerBudget(64, 32L << 20, 60_000_000_000L), () -> 101).status());
        assertEquals(applied, store.latestSequenceNumber());
        assertEquals(2, acknowledgements.get());
        assertEquals(1, authorityResolutions.get());
    }

    private static void terminalOutcomeFixture(
            TargetStoreBackend backend, ShardStore store, TargetQuotaScope scope, byte[] lineage,
            TargetMessageLocator locator, boolean historical) {
        final var priorSource = (KafkaSourcePosition) store.appliedShardLogPosition();
        final var at = source(priorSource, priorSource.offset() + 1, priorSource.brokerPersistenceTimeEpochMs() + 1);
        final byte[] digest = bytes(32, 0xB2);
        new TargetMessageStore(backend, 1, 1, 1).applyAccounted(budget(), reader -> {
            final byte[] key = TargetKeyCodec.message(locator.messageId());
            final var before = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                    reader.get(ColumnFamily.ID, key), TargetMessageRecord.VALUE_TYPE).payload());
            final var prior = before.runtime();
            final var terminalRuntime = new TargetGenerationRuntimeIndex(prior.generation(),
                    historical ? GenerationAggregateState.SUPERSEDED : GenerationAggregateState.DEAD_LETTER,
                    CurrentSendWorkKind.NONE, null, null, null, prior.attemptObligations(), prior.admissionsUsed(),
                    prior.uncertainRetryAdmissionsUsed(), prior.possibleDestinationDuplicate(),
                    prior.runtimeRevision() + 1);
            final var nextLocator = historical ? new TargetMessageLocator(locator.messageId(), locator.generation() + 1,
                    locator.target(), locator.domain(), locator.accountingIncarnation(), locator.orderingMode(),
                    locator.orderingDomain(), locator.scheduleBindingDigest()) : locator;
            final var work = historical ? new TargetTimelineWorkRef(nextLocator, TimelineWorkKind.INITIAL_SCHEDULE,
                    before.deliverAtEpochMs() + 1, before.deliverAtEpochMs() + 1, at.sourceOrderToken(),
                    1, 1, UncertainRetryAuthority.NONE, null, null, false) : null;
            final var after = new TargetMessageRecord(nextLocator, before.stateVersion() + 1,
                    historical ? before.deliverAtEpochMs() + 1 : before.deliverAtEpochMs(), before.expireAtEpochMs(),
                    historical ? before.deliverAtEpochMs() + 1 : before.retryEligibilityAtEpochMs(),
                    before.nativeDeliveryPolicy(), historical ? at : before.scheduleSource(), before.inlinePayload(),
                    before.payloadReference(), historical ? new TargetGenerationRuntimeIndex(nextLocator.generation(),
                            GenerationAggregateState.SCHEDULED, CurrentSendWorkKind.TIMELINE, work,
                            null, null, List.of(), 0, 0, false, 1) : terminalRuntime);
            final var terminal = new TargetTerminalGenerationRecord(locator, after.stateVersion(),
                    historical ? StableCode.SUPERSEDED : StableCode.DESTINATION_DEFINITIVE_PERMANENT, terminalRuntime,
                    new TargetQuotaMutation(reader.aggregate().mutation().sequence() + 1, at, digest), lineage);
            final byte[] orderKey = TargetKeyCodec.orderState(locator.target(), locator.orderingDomain());
            final var order = TargetOrderState.decode(TargetValueEnvelope.decode(
                    reader.get(ColumnFamily.META, orderKey), TargetOrderState.VALUE_TYPE).payload());
            order.requireBarrierProjection(before);
            final var nextOrder = new TargetOrderState(order.target(), order.orderingDomain(), order.sourceShard(),
                    order.executionDomain(), order.accountingIncarnation(), order.orderingContract(),
                    order.stateRevision() + 1, order.controlVersion(), order.gate(),
                    order.lastAdmittedOrder().encodedKey(), null,
                    historical ? new TargetOrderBarrier(locator, order.barrier().order().encodedKey(),
                            terminalRuntime.runtimeRevision(), terminalRuntime.runtimeDigest())
                            : TargetOrderBarrier.fromMessage(after));
            return new TargetMessageStore.Input(List.of(new TargetMessageStore.Transition(before, after)),
                    List.of(new TargetMessageStore.OrderTransition(order, nextOrder)),
                    List.of(reader.replace(ColumnFamily.TERMINAL, terminal.key(),
                            TargetTerminalGenerationRecord.VALUE_TYPE, terminal.canonicalBytes())));
        }, new TargetSourceAccounting(scope, lineage, at, digest, 16, 1, 1), (a, b, c) -> guard());
    }

    private static void assertPublishedTargetOutcome(
            TargetStoreBackend backend,
            ShardStore store,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetWorkerShardRuntime worker,
            java.util.concurrent.atomic.AtomicReference<SourceRecordConsumer.PolledSourceRecord> pending,
            TargetAdmissionFixture admission,
            KeyPair keys,
            OwnerIdentity owner,
            java.util.concurrent.atomic.AtomicReference<com.nereusstream.delay.protocol.PublishEvidence> expected,
            java.util.concurrent.atomic.AtomicInteger authorityResolutions,
            boolean mismatchedTransfer,
            WorkClassExecutionRegistry workerClasses, int publishFailure) {
        final var body = TargetPublishAdmissionBody.decode(admission.entry().mutation().canonicalBody());
        final var publication = body.publication();
        final var before = (KafkaSourcePosition) store.appliedShardLogPosition();
        final int sourceGap = publishFailure == 9 ? 2 : 1;
        final var at = source(before, before.offset() + sourceGap, before.brokerPersistenceTimeEpochMs() + sourceGap);
        final long observed = Math.max(at.brokerPersistenceTimeEpochMs(), body.decisionTime().latestEpochMs());
        final var time = new TrustedUtcIntervalEvidence(
                observed, observed + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                bytes(32, 0xA1), 1, 1, 1, bytes(32, 0xA2), 0, null);
        final boolean notPublished = publishFailure >= 6;
        final boolean permanent = publishFailure == 6;
        final boolean closed = publishFailure == 9;
        final StableCode cause = notPublished
                ? permanent ? StableCode.DESTINATION_DEFINITIVE_PERMANENT : StableCode.DESTINATION_DEFINITIVE_RETRIABLE
                : StableCode.OK;
        final int disposition = notPublished ? permanent ? 2 : publishFailure == 8 ? 3 : 1 : 0;
        final var evidence = notPublished ? targetRejectedEvidence(publication, publication.preparedPublishHash(),
                publication.physical().physicalPartition())
                : targetPublishedAck(publication, publication.preparedPublishHash());
        expected.set(evidence);
        final var policy = targetOutcomeRetryPolicy(false);
        final var history = new TargetPublishOutcomeVerifier.RetryContext(
                admission.entry().mutation(), admission.entry().mutation(), policy);
        final long retryAt = time.latestEpochMs() + RetryJitter.delayMs(
                RetryJitter.MESSAGE_PUBLISH, body.locator().messageId(), body.locator().generation(),
                body.attemptNo(), policy.retryBackoffCap(body.attemptNo()));
        final byte[] retry = com.nereusstream.delay.protocol.CanonicalProtobuf.message(out -> {
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(out, 1,
                    notPublished ? permanent ? 3 : publishFailure == 8 ? 4 : 2 : 1);
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 2, policy.ref().canonicalBytes());
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(out, 3, body.attemptNo());
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint64(out, 4, body.decisionTime().latestEpochMs());
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint64(out, 5, Math.min(
                    publication.expireAtEpochMs(), body.decisionTime().latestEpochMs() + policy.maxRetryDurationMs()));
            if (notPublished && !permanent) {
                com.nereusstream.delay.protocol.CanonicalProtobuf.uint64(out, 6, retryAt);
            }
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(out, 7, 1);
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(out, 8, cause.wireValue());
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(out, 9, 1);
        });
        final byte[] encoded = PublishOutcomeBody.encodeInitial(
                scope.shard(), time.latestEpochMs() + 10_000, body.publishAttemptId(),
                notPublished ? 2 : 1, disposition, cause, evidence.canonicalBytes(),
                body.outcomeTransfer(), time, retry);
        final var mutation = SystemMutation.signed(
                scope.shard(), SystemMutationType.PUBLISH_OUTCOME, time.latestEpochMs() + 10_000,
                body.publishAttemptId(), encoded,
                AuthorIdentity.owner(owner.deploymentId(), owner.workerRunId(), owner.ownerEpoch(),
                                owner.leaseFencingDigest()).canonicalBytes(), 1, keys.getPrivate());
        final var outcomes = new TargetPublishOutcomeStore(backend, scope, lineage, 16, 1, 1);
        final long originalSequence = store.latestSequenceNumber();
        assertThrows(IllegalStateException.class, () -> outcomes.prepareFirst(
                budget(), mutation, at, (a, b, c, d) -> new TargetPublishOutcomeVerifier.Authorization(
                        keys.getPublic(), ProtocolTuple.currentSystemMutation(), owner,
                        10, 10, 100, (e, f, g, h) -> true)));
        assertThrows(IllegalStateException.class, () -> outcomes.prepareFirst(
                budget(), mutation, at, (a, b, c, d) -> new TargetPublishOutcomeVerifier.Authorization(
                        keys.getPublic(), ProtocolTuple.currentSystemMutation(), owner,
                        10, 10, 100, (e, f, g, h) -> true, history,
                        new TargetPublishOutcomeVerifier.EvidenceContext(admission.entry().mutation(),
                                (p, e, first, source) -> {
                                    throw new IllegalStateException("fixture authenticated response is unavailable");
                                }))));
        assertEquals(originalSequence, store.latestSequenceNumber());
        assertEquals(before, store.appliedShardLogPosition());
        assertNull(store.get(ColumnFamily.DEDUPE, systemKey(mutation)));
        final byte[] messageKey = TargetKeyCodec.message(body.locator().messageId());
        final var message = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                store.get(ColumnFamily.ID, messageKey), TargetMessageRecord.VALUE_TYPE).payload());
        final byte[] orderKey = TargetKeyCodec.orderState(body.locator().target(), body.locator().orderingDomain());
        final var order = TargetOrderState.decode(TargetValueEnvelope.decode(
                store.get(ColumnFamily.META, orderKey), TargetOrderState.VALUE_TYPE).payload());
        if (notPublished) {
            assertThrows(IllegalArgumentException.class, () -> targetRejectedEvidence(
                    publication, bytes(32, 0xEF), publication.physical().physicalPartition())
                    .requireOrdinaryTargetNotPublishedBinding(publication));
            assertThrows(IllegalArgumentException.class, () -> targetRejectedEvidence(
                    publication, publication.preparedPublishHash(), publication.physical().physicalPartition() + 1)
                    .requireOrdinaryTargetNotPublishedBinding(publication));
        }
        final var bridge = mismatchedTransfer ? null : publishThroughTargetBridge(
                worker, workerClasses, admission, keys, time, retry, expected, at, publishFailure);
        if (closed) {
            closeRetryQueueFixture(backend, store, scope, lineage, body.locator().target());
        }
        final var actualMutation = mismatchedTransfer
                ? SystemMutation.signed(
                        scope.shard(), SystemMutationType.PUBLISH_OUTCOME, time.latestEpochMs() + 10_000,
                        body.publishAttemptId(), PublishOutcomeBody.encodeInitial(
                                scope.shard(), time.latestEpochMs() + 10_000, body.publishAttemptId(),
                                1, 0, StableCode.OK, evidence.canonicalBytes(),
                                new PublishAdmissionBody.ChargeVector(
                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0).canonicalBytes(),
                                time, retry),
                        mutation.authorIdentity(), 1, keys.getPrivate())
                : bridge.publisher().mutation().orElseThrow();
        final var entry = new SourceReplayMutation(actualMutation, at, null, null);
        final var acknowledgements = new java.util.concurrent.atomic.AtomicInteger();
        pending.set(new SourceRecordConsumer.PolledSourceRecord(entry, (actual, result) -> {
            assertEquals(entry, actual);
            assertEquals(mismatchedTransfer ? ApplyStatus.REJECTED : ApplyStatus.APPLIED,
                    result.systemMutationResult().applyStatus());
            assertEquals(mismatchedTransfer ? StableCode.STALE_SYSTEM_MUTATION
                            : closed ? StableCode.LANE_CLOSED_AFTER_ADMISSION_NOT_PUBLISHED
                            : PublishOutcomeBody.decode(actualMutation.canonicalBody()).stableCode(),
                    result.systemMutationResult().stableCode());
            return acknowledgements.incrementAndGet() == 1
                    ? SourceAcknowledgement.AcknowledgementResult.unknown(null)
                    : SourceAcknowledgement.AcknowledgementResult.acked();
        }));
        assertEquals(SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN,
                worker.runSourceTurn(new SchedulerBudget(64, 32L << 20, 60_000_000_000L), () -> 101).status());
        assertEquals(entry, worker.pendingSourceEntry().orElseThrow());
        assertEquals(at, store.appliedShardLogPosition());
        final var after = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                store.get(ColumnFamily.ID, messageKey), TargetMessageRecord.VALUE_TYPE).payload());
        if (mismatchedTransfer) {
            assertArrayEquals(message.canonicalBytes(), after.canonicalBytes());
            assertArrayEquals(order.canonicalBytes(), TargetOrderState.decode(TargetValueEnvelope.decode(
                    store.get(ColumnFamily.META, orderKey), TargetOrderState.VALUE_TYPE).payload()).canonicalBytes());
            final byte[] rejectedBudgetKey = Bytes.concat(
                    new byte[] {TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG, TargetKeyCodec.KEY_FORMAT},
                    body.publishAttemptId());
            final var stillAdmitted = TargetQuotaAttemptBudget.decode(TargetValueEnvelope.decode(
                    store.get(ColumnFamily.META, rejectedBudgetKey), TargetQuotaAttemptBudget.VALUE_TYPE).payload());
            assertEquals(TargetQuotaAttemptBudget.Phase.ADMITTED, stillAdmitted.phase());
            assertEquals(1, stillAdmitted.revision());
            assertNull(store.get(ColumnFamily.TERMINAL, TargetTerminalGenerationRecord.key(after.locator())));
            final var rejected = TargetResultRecord.decode(TargetValueEnvelope.decode(
                    store.get(ColumnFamily.DEDUPE, systemKey(actualMutation)),
                    TargetResultRecord.VALUE_TYPE).payload());
            assertEquals(StableCode.STALE_SYSTEM_MUTATION,
                    SystemMutationResult.decode(rejected.typedPayload()).stableCode());
            final long appliedSequence = store.latestSequenceNumber();
            assertEquals(SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED,
                    worker.runSourceTurn(new SchedulerBudget(64, 32L << 20, 60_000_000_000L), () -> 101).status());
            assertEquals(appliedSequence, store.latestSequenceNumber());
            assertEquals(2, acknowledgements.get());
            assertEquals(1, authorityResolutions.get());
            return;
        }
        if (publishFailure > 0 && publishFailure < 6) {
            assertEquals(GenerationAggregateState.UNCERTAIN, after.aggregateState());
            assertEquals(CurrentSendWorkKind.NONE, after.runtime().currentWorkKind());
            final byte[] unknownKey = Bytes.concat(
                    new byte[] {TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG, TargetKeyCodec.KEY_FORMAT},
                    body.publishAttemptId());
            final var unknown = TargetQuotaAttemptBudget.decode(TargetValueEnvelope.decode(
                    store.get(ColumnFamily.META, unknownKey), TargetQuotaAttemptBudget.VALUE_TYPE).payload());
            assertEquals(TargetQuotaAttemptBudget.Phase.UNKNOWN, unknown.phase());
            assertEquals(body.commitment(), unknown.commitment());
            assertEquals(body.allocated(), unknown.allocated());
            assertEquals(1, unknown.effectiveCharge().amount(CapacityDimension.INFLIGHT_MESSAGES));
            assertNull(store.get(ColumnFamily.TERMINAL, TargetTerminalGenerationRecord.key(after.locator())));
            final long applied = store.latestSequenceNumber();
            assertEquals(SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED,
                    worker.runSourceTurn(new SchedulerBudget(64, 32L << 20, 60_000_000_000L), () -> 101).status());
            assertEquals(applied, store.latestSequenceNumber());
            assertEquals(2, acknowledgements.get());
            assertTrue(bridge.executor().settleApplied(bridge.handoff(), () -> 101));
            return;
        }
        if (notPublished) {
            assertNotPublishedProjection(store, body, after, order,
                    closed ? StableCode.LANE_CLOSED_AFTER_ADMISSION_NOT_PUBLISHED : cause, permanent || closed);
        } else {
            assertArrayEquals(message.publishedOutcome(body.publishAttemptId()).canonicalBytes(),
                    after.canonicalBytes());
            assertEquals(GenerationAggregateState.PUBLISHED, after.aggregateState());
            final var afterOrder = TargetOrderState.decode(TargetValueEnvelope.decode(
                    store.get(ColumnFamily.META, orderKey), TargetOrderState.VALUE_TYPE).payload());
            assertArrayEquals(order.afterPublishedOutcome(message, after).canonicalBytes(),
                    afterOrder.canonicalBytes());
            assertNull(afterOrder.barrier());
            final byte[] budgetKey = Bytes.concat(
                    new byte[] {TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG, TargetKeyCodec.KEY_FORMAT},
                    body.publishAttemptId());
            final var resolved = TargetQuotaAttemptBudget.decode(TargetValueEnvelope.decode(
                    store.get(ColumnFamily.META, budgetKey), TargetQuotaAttemptBudget.VALUE_TYPE).payload());
            assertEquals(TargetQuotaAttemptBudget.Phase.RESOLVED_AWAITING_FLOOR, resolved.phase());
            assertEquals(body.commitment(), resolved.commitment());
            assertEquals(body.allocated(), resolved.allocated());
            assertEquals(0, resolved.effectiveCharge().amount(CapacityDimension.INFLIGHT_MESSAGES));
            assertNull(resolved.floorDigest());
            final byte[] payloadKey = Bytes.concat(
                    new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, TargetKeyCodec.KEY_FORMAT},
                    body.locator().messageId().bytes());
            final var retained = TargetQuotaPayloadOwner.decode(TargetValueEnvelope.decode(
                    store.get(ColumnFamily.META, payloadKey), TargetQuotaPayloadOwner.VALUE_TYPE).payload());
            assertEquals(TargetQuotaPayloadOwner.Phase.RETAINED, retained.phase());
            retained.requireMessagePayload(after);
            final var terminal = TargetTerminalGenerationRecord.decode(TargetValueEnvelope.decode(
                    store.get(ColumnFamily.TERMINAL, TargetTerminalGenerationRecord.key(after.locator())),
                    TargetTerminalGenerationRecord.VALUE_TYPE).payload());
            terminal.requireOwner(retained);
            assertEquals(after.runtime(), terminal.runtime());
            final var first = TargetResultRecord.decode(TargetValueEnvelope.decode(
                    store.get(ColumnFamily.DEDUPE, systemKey(actualMutation)),
                    TargetResultRecord.VALUE_TYPE).payload());
            assertArrayEquals(at.canonicalBytes(),
                    SystemMutationResult.decode(first.typedPayload()).appliedSourcePosition());
            final var position = TargetResultRecord.decode(TargetValueEnvelope.decode(
                    store.get(ColumnFamily.DEDUPE, Bytes.concat(
                            new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT},
                            at.canonicalBytes())),
                    TargetResultRecord.VALUE_TYPE).payload());
            position.requireFirst(first);
        }
        final long appliedSequence = store.latestSequenceNumber();
        assertEquals(SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED,
                worker.runSourceTurn(new SchedulerBudget(64, 32L << 20, 60_000_000_000L), () -> 101).status());
        assertEquals(2, acknowledgements.get());
        assertEquals(1, authorityResolutions.get());
        assertEquals(appliedSequence, store.latestSequenceNumber());
        assertTrue(worker.pendingSourceEntry().isEmpty());
        assertTrue(bridge.executor().settleApplied(bridge.handoff(), () -> 101));
        assertThrows(IllegalStateException.class,
                () -> worker.readAppliedAdmission(budget(), admission.entry().mutation(), () -> 101));
    }

    private static void assertNotPublishedProjection(
            ShardStore store, TargetPublishAdmissionBody body, TargetMessageRecord after,
            TargetOrderState order, StableCode cause, boolean permanent) {
        assertEquals(permanent ? GenerationAggregateState.DEAD_LETTER : GenerationAggregateState.RETRY_WAIT,
                after.aggregateState());
        assertTrue(after.runtime().attemptObligations().isEmpty());
        final byte[] orderKey = TargetKeyCodec.orderState(body.locator().target(), body.locator().orderingDomain());
        final var nextOrder = TargetOrderState.decode(TargetValueEnvelope.decode(
                store.get(ColumnFamily.META, orderKey), TargetOrderState.VALUE_TYPE).payload());
        assertArrayEquals(order.lastAdmittedOrder().encodedKey(), nextOrder.lastAdmittedOrder().encodedKey());
        assertNull(nextOrder.barrier());
        final byte[] budgetKey = Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG, TargetKeyCodec.KEY_FORMAT},
                body.publishAttemptId());
        final var resolved = TargetQuotaAttemptBudget.decode(TargetValueEnvelope.decode(
                store.get(ColumnFamily.META, budgetKey), TargetQuotaAttemptBudget.VALUE_TYPE).payload());
        assertEquals(TargetQuotaAttemptBudget.Phase.RESOLVED_AWAITING_FLOOR, resolved.phase());
        assertEquals(body.commitment(), resolved.commitment());
        assertEquals(body.allocated(), resolved.allocated());
        assertEquals(0, resolved.effectiveCharge().amount(CapacityDimension.INFLIGHT_MESSAGES));
        assertNull(resolved.floorDigest());
        final byte[] payloadKey = Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, TargetKeyCodec.KEY_FORMAT},
                body.locator().messageId().bytes());
        final var payload = TargetQuotaPayloadOwner.decode(TargetValueEnvelope.decode(
                store.get(ColumnFamily.META, payloadKey), TargetQuotaPayloadOwner.VALUE_TYPE).payload());
        assertEquals(permanent ? TargetQuotaPayloadOwner.Phase.RETAINED : TargetQuotaPayloadOwner.Phase.ACTIVE,
                payload.phase());
        final byte[] rawTerminal = store.get(
                ColumnFamily.TERMINAL, TargetTerminalGenerationRecord.key(after.locator()));
        if (permanent) {
            assertEquals(CurrentSendWorkKind.NONE, after.runtime().currentWorkKind());
            final var terminal = TargetTerminalGenerationRecord.decode(
                    TargetValueEnvelope.decode(rawTerminal, TargetTerminalGenerationRecord.VALUE_TYPE).payload());
            assertEquals(cause, terminal.terminalCode());
            assertEquals(after.runtime(), terminal.runtime());
            terminal.requireOwner(payload);
        } else {
            assertNull(rawTerminal);
            final var work = after.runtime().timeline();
            assertEquals(TimelineWorkKind.DEFINITIVE_RETRY, work.workKind());
            assertEquals(body.attemptNo() + 1, work.candidateAttemptNo());
            assertFalse(work.nativeCandidate());
            assertNotNull(nextOrder.serviceableHead());
            assertArrayEquals(work.canonicalBytes(), TargetValueEnvelope.decode(
                    store.get(ColumnFamily.TIMELINE, work.ordinaryKey()), TargetTimelineWorkRef.VALUE_TYPE).payload());
            assertEquals(after.locator().messageId(), nextOrder.serviceableHead().messageId());
        }
    }

    private static com.nereusstream.delay.protocol.PublishEvidence targetRejectedEvidence(
            com.nereusstream.delay.protocol.TargetOrdinaryPublicationBinding publication,
            byte[] preparedHash, long partition) {
        final byte[] branch = com.nereusstream.delay.protocol.CanonicalProtobuf.message(out -> {
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(out, 1,
                    com.nereusstream.delay.protocol.AdapterKind.PULSAR.wireValue());
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 2,
                    publication.physical().resource().canonicalBytes());
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(out, 3, partition);
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 4,
                    com.nereusstream.delay.protocol.ExternalDeliveryIdentity
                            .publishAttempt(publication.publishAttemptId()).canonicalBytes());
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 5, preparedHash);
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 6, bytes(32, 0xE1));
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(out, 7, 1);
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 8, bytes(32, 0xE2));
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(out, 9, 1);
        });
        return com.nereusstream.delay.protocol.PublishEvidence.create(
                com.nereusstream.delay.protocol.PublishEvidenceKind.BROKER_DEFINITIVE_REJECTION,
                com.nereusstream.delay.protocol.EvidenceVerificationStatus.VERIFIED_NOT_PUBLISHED, branch);
    }

    private record PublishBridge(
            com.nereusstream.delay.ownership.TargetPulsarPublishExecutor executor,
            com.nereusstream.delay.ownership.TargetPulsarPublishExecutor.Submission publisher,
            com.nereusstream.delay.ownership.TargetOutcomeWorkClassExecutor handoff) {}

    private static PublishBridge publishThroughTargetBridge(
            TargetWorkerShardRuntime worker, WorkClassExecutionRegistry classes, TargetAdmissionFixture admission,
            KeyPair keys, TrustedUtcIntervalEvidence time, byte[] none,
            java.util.concurrent.atomic.AtomicReference<com.nereusstream.delay.protocol.PublishEvidence> evidence,
            KafkaSourcePosition sourceAt, int publishFailure) {
        final var applied = worker.readAppliedAdmission(budget(), admission.entry().mutation(), () -> 101);
        final var publication = applied.body().publication();
        final var artifacts = com.nereusstream.delay.protocol.ArtifactGenerationSet.current(
                1, com.nereusstream.delay.protocol.PulsarSourceLock.digest(), bytes(32, 0xC3));
        final var pool = new com.nereusstream.delay.adapter.DestinationPhysicalAdmission(1, 1_000_000);
        pool.registerTargetCluster(publication.physical().resource().pulsar().authenticatedClusterId(), 1, 1_000_000);
        pool.registerTargetChannel(new com.nereusstream.delay.adapter.DestinationPhysicalAdmission.TargetChannelSpec(
                publication.channel(), publication.physical(), 1, 1_000_000, 1, 1_000_000));
        if (publishFailure != 4) {
            pool.openTargetReady(publication.channel());
        }
        final var sends = new java.util.concurrent.atomic.AtomicInteger();
        final com.nereusstream.delay.adapter.DestinationPublishAdapter delegate =
                new com.nereusstream.delay.adapter.DestinationPublishAdapter() {
                    @Override
                    public java.util.concurrent.CompletionStage<com.nereusstream.delay.adapter.DestinationPublishResult>
                            publish(com.nereusstream.delay.adapter.DestinationPublishRequest request) {
                        throw new AssertionError("Target bridge must not use a Lane request");
                    }

                    @Override
                    public java.util.concurrent.CompletionStage<com.nereusstream.delay.adapter.DestinationPublishResult>
                            publishPreparedRecord(com.nereusstream.delay.protocol.PulsarPreparedRecord record,
                                    com.nereusstream.delay.protocol.ArtifactGenerationSet set,
                                    com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter
                                            .PreparedPublishPreflight gate) {
                        final var blocked = gate.check(record, set);
                        if (blocked != null) {
                            return java.util.concurrent.CompletableFuture.completedFuture(blocked);
                        }
                        sends.incrementAndGet();
                        if (publishFailure >= 6) {
                            return java.util.concurrent.CompletableFuture.completedFuture(
                                    com.nereusstream.delay.adapter.DestinationPublishResult.definitelyNotPublished(
                                            publishFailure == 6 ? StableCode.DESTINATION_DEFINITIVE_PERMANENT
                                                    : StableCode.DESTINATION_DEFINITIVE_RETRIABLE,
                                            evidence.get().canonicalBytes()));
                        }
                        final var ack = com.nereusstream.delay.adapter.PulsarSendAckEvidence.publishedRecord(
                                record, set, record.sequenceAuthority().producerNameHash(), 1, 2, 0, 1,
                                time.earliestEpochMs(), 21, 1, 1, record.sequenceAuthority().sequenceId(),
                                bytes(32, 0xDA), bytes(32, 0xDB));
                        evidence.set(ack);
                        return java.util.concurrent.CompletableFuture.completedFuture(
                                com.nereusstream.delay.adapter.DestinationPublishResult.published(
                                        publication.physical().resource(),
                                        Math.toIntExact(publication.physical().physicalPartition()),
                                        publication.publishAttemptId(), time.earliestEpochMs(), ack.canonicalBytes()));
                    }
                };
        final var adapter = new com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter(
                delegate, pool, classes, Runnable::run);
        final var entry = new java.util.concurrent.atomic.AtomicInteger();
        final var fresh = com.nereusstream.delay.adapter.PulsarAttemptJournal.forTargets(
                applied.body().shard(), request -> {
                    if (publishFailure == 5 || publishFailure == 1
                            && request.kind() == com.nereusstream.delay.adapter.PulsarAttemptJournal
                                    .RecordKind.OWNERSHIP_STARTED) {
                        throw new IllegalStateException("fixture uncertain ownership append");
                    }
                    return new com.nereusstream.delay.adapter.PulsarAttemptJournal.JournalPosition(
                            1, entry.getAndIncrement(), 0, 1, time.earliestEpochMs());
                }, null, 16, 1_000_000);
        final var journal = publishFailure == 3
                ? com.nereusstream.delay.adapter.PulsarAttemptJournal.forTargets(applied.body().shard(),
                        request -> { throw new AssertionError("recovered ownership must not append or resend"); },
                        null, 16, 1_000_000)
                : fresh;
        if (publishFailure == 3) {
            final var p = com.nereusstream.delay.protocol.PayloadForPublish.inline(applied.message().inlinePayload());
            final var mapped = fresh.appendOrReuseCurrent(
                    com.nereusstream.delay.adapter.PulsarAttemptJournal.ProducerKey.target(
                            publication.channel(), publication.physical()),
                    com.nereusstream.delay.adapter.PulsarPreparedRecordFactory.targetJournalIdentity(
                            publication, applied.message(), p, applied.source())).record().mapping();
            fresh.markOwnershipStarted(mapped);
            fresh.records().forEach(journal::replay);
        }
        final var source = new com.nereusstream.delay.ownership.TargetOutcomeWorkClassExecutor(
                worker, image -> com.nereusstream.delay.ownership.ShardLogMutationAppender.AppendOutcome
                        .persisted(sourceAt));
        final var handoffs = new java.util.concurrent.atomic.AtomicInteger();
        final var executor = new com.nereusstream.delay.ownership.TargetPulsarPublishExecutor(
                journal, adapter, artifacts,
                () -> worker.readAppliedAdmission(budget(), admission.entry().mutation(), () -> 101),
                (p, r, a) -> publishFailure == 2
                        ? com.nereusstream.delay.adapter.DestinationPublishResult.unknown(
                                StableCode.CAPABILITY_UNAVAILABLE, null) : null,
                (a, r, result) -> {
                    final boolean unknown = result.disposition()
                            == com.nereusstream.delay.adapter.DestinationPublishResult.Disposition.UNKNOWN;
                    final var policy = targetOutcomeRetryPolicy(false);
                    final byte[] retry = unknown ? typedUnknownRetryDecision(
                            policy, a.body().decisionTime().latestEpochMs(), Math.min(publication.expireAtEpochMs(),
                                    a.body().decisionTime().latestEpochMs() + policy.maxRetryDurationMs()),
                            a.body().attemptNo(), null, result.stableCode()) : none;
                    return new com.nereusstream.delay.ownership.WorkerPublishOutcomeMutationFactory.OutcomeContext(
                            time.latestEpochMs() + 10_000,
                            unknown ? 4 : publishFailure >= 6
                                    ? publishFailure == 6 ? 2 : publishFailure == 8 ? 3 : 1 : 0,
                            a.body().outcomeTransfer(), time, retry);
                },
                new com.nereusstream.delay.ownership.TargetPublishOutcomeMutationFactory(1, keys.getPrivate()),
                image -> {
                    final var retained = source.submit(image, () -> 101);
                    assertEquals(retained, source.submit(image, () -> 101));
                    if (handoffs.incrementAndGet() == 1) {
                        throw new IllegalStateException("fixture lost queue-acceptance response");
                    }
                }, Runnable::run);
        final var payload = com.nereusstream.delay.protocol.PayloadForPublish.inline(applied.message().inlinePayload());
        final var result = executor.submit(payload,
                com.nereusstream.delay.protocol.ResolvedPayload.of(applied.message().inlinePayload()));
        assertNotNull(result.failure().orElse(null));
        assertEquals(publishFailure == 0 || publishFailure >= 6 ? 1 : 0, sends.get());
        assertEquals(publishFailure == 0 ? 3 : publishFailure == 5 ? 0 : publishFailure == 1 ? 1 : 2,
                journal.records().size());
        final byte[] exact = result.mutation().orElseThrow().encodeFrame();
        assertEquals(result, executor.submit(payload,
                com.nereusstream.delay.protocol.ResolvedPayload.of(applied.message().inlinePayload())));
        executor.retryCompletion();
        assertTrue(result.handedOff());
        assertArrayEquals(exact, result.mutation().orElseThrow().encodeFrame());
        classes.runTurn(new SchedulerBudget(100, 2_000_000, 60_000_000_000L));
        assertEquals(publishFailure == 0 || publishFailure >= 6 ? 1 : 0, sends.get());
        assertEquals(0, pool.workerSnapshot().activeRequests());
        return new PublishBridge(executor, result, source);
    }

    private static com.nereusstream.delay.protocol.PublishEvidence targetPublishedAck(
            com.nereusstream.delay.protocol.TargetOrdinaryPublicationBinding publication, byte[] preparedHash) {
        final byte[] branch = com.nereusstream.delay.protocol.CanonicalProtobuf.message(out -> {
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 1,
                    publication.physical().resource().canonicalBytes());
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(out, 2,
                    publication.physical().physicalPartition());
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint64(out, 3, 1);
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint64(out, 4, 2);
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(out, 5, 0);
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint64(out, 6, publication.deliverAtEpochMs());
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 7, bytes(32, 0xD1));
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint64(out, 8, 1);
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 9,
                    com.nereusstream.delay.protocol.ExternalDeliveryIdentity
                            .publishAttempt(publication.publishAttemptId()).canonicalBytes());
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 10, preparedHash);
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 11, bytes(32, 0xD2));
        });
        return com.nereusstream.delay.protocol.PublishEvidence.create(
                com.nereusstream.delay.protocol.PublishEvidenceKind.PULSAR_SEND_ACK,
                com.nereusstream.delay.protocol.EvidenceVerificationStatus.VERIFIED_PUBLISHED, branch);
    }

    private static com.nereusstream.delay.protocol.TargetChannelIdentity materializedChannelFixture(
            TargetStoreBackend backend,
            ShardStore store,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetClaimRecord claim,
            TargetScheduleBinding binding,
            ProfileRef providerProfile) {
        final var prior = (KafkaSourcePosition) store.appliedShardLogPosition();
        final var at = source(prior, prior.offset() + 1, prior.brokerPersistenceTimeEpochMs() + 1);
        final var context = new com.nereusstream.delay.protocol.TargetChannelIdentity.Context(
                scope.shard(), binding.target(), binding.domain(), binding.accountingIncarnation(),
                binding.offeredDispatchRef(), binding.controlScopeRef(),
                com.nereusstream.delay.protocol.ChannelKind.PULSAR_DEDUP_PRODUCER,
                0, 1, 1L, bytes(32, 0xC1));
        final var issued = new TrustedUtcIntervalEvidence(
                at.brokerPersistenceTimeEpochMs(), at.brokerPersistenceTimeEpochMs() + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                bytes(32, 0xA1), 1, 1, 1, bytes(32, 0xA2), 0, null);
        final var channel = new com.nereusstream.delay.protocol.TargetChannelIdentity(
                context,
                new com.nereusstream.delay.protocol.CredentialUseLease(
                        providerProfile, com.nereusstream.delay.protocol.CredentialUseKind.DESTINATION_CHANNEL,
                        context.credentialHolderScope(), 1, bytes(32, 0xC2), bytes(32, 0xC3),
                        issued, claim.deadlineEpochMs() + 1000, 1));
        new TargetMessageStore(backend, 1, 1, 1).applyAccounted(
                budget(),
                reader -> new TargetMessageStore.Input(List.of(), List.of(), List.of(reader.replace(
                        ColumnFamily.META, channel.encodedKey(),
                        com.nereusstream.delay.protocol.TargetChannelIdentity.VALUE_TYPE, channel.canonicalBytes()))),
                new TargetSourceAccounting(scope, lineage, at, bytes(32, 0xC4), 16, 1, 1),
                (a, b, c) -> guard());
        return channel;
    }

    private static void closeRetryQueueFixture(
            TargetStoreBackend backend,
            ShardStore store,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetPartitionId target) {
        final var priorSource = (KafkaSourcePosition) store.appliedShardLogPosition();
        final var at = source(priorSource, priorSource.offset() + 1, priorSource.brokerPersistenceTimeEpochMs() + 1);
        final byte[] digest = bytes(32, 0xB1);
        new TargetMessageStore(backend, 1, 1, 1).applyAccounted(
                budget(),
                reader -> {
                    final byte[] key = TargetKeyCodec.state(target);
                    final var prior = TargetQueueState.decode(TargetValueEnvelope.decode(
                                    reader.get(ColumnFamily.META, key), TargetQueueState.VALUE_TYPE)
                            .payload());
                    final var domains = prior.domains().stream().map(domain -> new TargetDomainState(
                                    domain.domain(), domain.lifecycle(), domain.dispatchCompatibilityRef(),
                                    domain.controlScopeRef(), domain.nativePolicyScopeRef(), null, null))
                            .toList();
                    final var closed = new TargetQueueState(
                            target,
                            TargetQueueState.nextRevision(prior.headRevision()),
                            TargetQueueState.nextRevision(prior.controlVersion()),
                            TargetQueueState.AdmissionState.CLOSED,
                            prior.accountingIncarnation(),
                            prior.nativeIndexLeadCapMs(),
                            domains);
                    closed.requireSuccessorOf(prior);
                    return new TargetMessageStore.Input(
                            List.of(), List.of(),
                            List.of(reader.replace(
                                    ColumnFamily.META, key, TargetQueueState.VALUE_TYPE, closed.canonicalBytes())));
                },
                new TargetSourceAccounting(scope, lineage, at, digest, 16, 1, 1),
                (a, b, c) -> guard());
    }

    private static com.nereusstream.delay.protocol.RetryPolicySemantic targetOutcomeRetryPolicy(boolean retry) {
        return new com.nereusstream.delay.protocol.RetryPolicySemantic(
                Bytes.utf8("target-outcome-policy"),
                1,
                10,
                100,
                3,
                60_000,
                retry
                        ? com.nereusstream.delay.protocol.UncertainPolicy.BOUNDED_RETRY_POSSIBLE_DUPLICATE
                        : com.nereusstream.delay.protocol.UncertainPolicy.HOLD_FOR_EVIDENCE,
                retry ? 1 : 0,
                com.nereusstream.delay.protocol.DlqExportMode.NOT_CONFIGURED,
                0,
                0,
                0,
                0,
                false,
                bytes(32, 0xA3));
    }

    private static byte[] typedUnknownRetryDecision(
            com.nereusstream.delay.protocol.RetryPolicySemantic policy,
            long firstAttemptAt,
            long deadline,
            int completedAttempt,
            Long nextRetryAt) {
        return typedUnknownRetryDecision(policy, firstAttemptAt, deadline, completedAttempt,
                nextRetryAt, StableCode.RECOVERY_FIRST_SEND_UNCERTAIN);
    }

    private static byte[] typedUnknownRetryDecision(
            com.nereusstream.delay.protocol.RetryPolicySemantic policy,
            long firstAttemptAt,
            long deadline,
            int completedAttempt,
            Long nextRetryAt, StableCode cause) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, nextRetryAt == null ? 5 : 2);
            CanonicalProtobuf.bytes(output, 2, policy.ref().canonicalBytes());
            CanonicalProtobuf.uint32(output, 3, completedAttempt);
            CanonicalProtobuf.uint64(output, 4, firstAttemptAt);
            CanonicalProtobuf.uint64(output, 5, deadline);
            if (nextRetryAt != null) {
                CanonicalProtobuf.uint64(output, 6, nextRetryAt);
            }
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, cause.wireValue());
            CanonicalProtobuf.uint32(output, 9, 1);
        });
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

    private static SystemMutationResult applyExpiry(
            WorkerSourceApplyLoop loop,
            java.util.ArrayDeque<SourceRecordConsumer.PolledSourceRecord> queue,
            SystemMutation mutation,
            KafkaSourcePosition position) {
        queue.add(new SourceRecordConsumer.PolledSourceRecord(
                new SourceReplayMutation(mutation, position, null, null),
                (entry, outcome) -> SourceAcknowledgement.AcknowledgementResult.acked()));
        final var turn = loop.runTurn(new SchedulerBudget(1, 1_000_000, 1_000), () -> 100);
        if (turn.failure() != null) {
            throw new AssertionError("Target expiry apply failed: " + turn.status(), turn.failure());
        }
        assertEquals(SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED, turn.status());
        return turn.appliedOutcome().systemMutationResult();
    }

    private static boolean expiryDiscoveryFinds(
            TargetExpiryDiscoveryStore discovery,
            TrustedUtcIntervalEvidence evidence,
            TargetExpiryDiscoveryStore.Candidate expected) {
        TargetExpiryDiscoveryStore.Cursor cursor = null;
        for (int attempts = 0; attempts < 256; attempts++) {
            final var step = discovery.discover(budget(), cursor, evidence, (a, b) -> guard());
            if (step.candidate().filter(expected::equals).isPresent()) {
                return true;
            }
            if (step.sweepComplete()) {
                return false;
            }
            cursor = step.nextCursor();
        }
        throw new AssertionError("Target expiry discovery did not finish its bounded test sweep");
    }

    private static SystemMutation expire(
            DelayMessageId messageId,
            long expireAt,
            KafkaSourcePosition source,
            AuthorIdentity owner,
            KeyPair keys,
            boolean wrongIdentity) {
        final long retryUntil = Math.addExact(source.brokerLogAppendTimeEpochMs(), 10_000);
        final var proof = new TrustedUtcIntervalEvidence(
                expireAt,
                Math.addExact(expireAt, 1),
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("target-expiry-test-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("target-expiry-test-proof")),
                0,
                null);
        final var body = new TargetExpireGenerationBody(
                source.shardId(), retryUntil, messageId, 0, expireAt, proof);
        final byte[] logicalId = wrongIdentity
                ? Bytes.sha256(Bytes.utf8("wrong-target-expiry-logical-id"))
                : body.logicalOperationIdentity();
        return SystemMutation.signed(
                source.shardId(),
                SystemMutationType.EXPIRE_GENERATION,
                retryUntil,
                logicalId,
                body.canonicalBytes(),
                owner.canonicalBytes(),
                1,
                keys.getPrivate());
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
