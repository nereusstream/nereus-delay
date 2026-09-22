package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.ownership.InMemoryControlTargetRegistrationAuthority;
import com.nereusstream.delay.ownership.InMemoryOwnerLeaseStore;
import com.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import com.nereusstream.delay.ownership.ShardLifecycleState;
import com.nereusstream.delay.ownership.SourceAcknowledgement;
import com.nereusstream.delay.ownership.SourceApplyCoordinator;
import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.ownership.SourceRecordConsumer;
import com.nereusstream.delay.ownership.SourceReplayRecord;
import com.nereusstream.delay.ownership.SourceReplaySuccessor;
import com.nereusstream.delay.ownership.TargetReservationQueryWorkClassExecutor;
import com.nereusstream.delay.ownership.TargetSourceApplyRuntime;
import com.nereusstream.delay.ownership.WorkerSourceApplyLoop;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CancelCommandBody;
import com.nereusstream.delay.protocol.CanonicalPayloadCommitProof;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.CommandHash;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.CommandType;
import com.nereusstream.delay.protocol.CommitLargeScheduleBody;
import com.nereusstream.delay.protocol.ControlAuthor;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
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
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.RescheduleCommandBody;
import com.nereusstream.delay.protocol.ScheduleCommandBody;
import com.nereusstream.delay.protocol.SelfRoutingId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
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
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
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
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.openTarget(config, scope.shard(), resources)) {
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

            final var assignment = new SourceAssignment(
                    scope.shard(),
                    bytes(32, 0x43),
                    1,
                    new KafkaActivationBarrier(
                            scope.shard(),
                            scheduleAt.authenticatedClusterId(),
                            scheduleAt.nativeTopicUuid(),
                            scheduleAt.offset()));
            final var leases = new OxiaOwnerLeaseStore(new InMemoryOwnerLeaseStore());
            final var active = leases.transition(
                            leases.acquire(assignment, "cancel-worker", bytes(32, 0x44), 1, 10000)
                                    .orElseThrow(),
                            ShardLifecycleState.ACTIVE_FOR_COMMANDS)
                    .orElseThrow();
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
            final var at = source(scheduleAt, scheduleAt.offset() + 1, scheduleAt.brokerLogAppendTimeEpochMs() + 1);
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
            final var queries = new TargetReservationQueryStore(backend, scope, lineage);
            final var location = new TargetReservationQueryStore.ReceiptLocation(
                    prepareBody.objectStoreProfile(), Bytes.utf8("bucket"), Bytes.utf8("object"));
            final long beforeQueries = store.latestSequenceNumber();
            final var reservedSnapshot = queries.complete(
                            queries.prepare(budget(), reserved.reservationId()), (a, b) -> guard())
                    .orElseThrow();
            assertEquals(
                    PayloadReservationStatus.RESERVED,
                    reservedSnapshot.reservation().status());
            assertEquals(TargetQuotaPayloadOwner.Phase.RESERVED, reservedSnapshot.payloadPhase());
            assertEquals(prepareAt, reservedSnapshot.readSource());
            final var prepareReceipt = reservedSnapshot.receipt(location);
            assertEquals(prepareAt, prepareReceipt.appliedSourcePosition());
            assertEquals(1, prepareReceipt.stateVersion());
            assertTrue(queries.complete(queries.prepare(budget(), bytes(32, 0xf6)), (a, b) -> guard())
                    .isEmpty());
            assertThrows(
                    IllegalStateException.class,
                    () -> queries.complete(queries.prepare(budget(), reserved.reservationId()), (a, b) -> {
                        throw new IllegalStateException("Owner lost");
                    }));
            final var localReadExhaustion = assertThrows(
                    com.nereusstream.delay.store.ReadIncompleteException.class,
                    () -> queries.prepare(
                            new BoundedReadBudget(1, 32L << 20, 60_000_000_000L, System::nanoTime),
                            reserved.reservationId()));
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
            final var staleQuery = queries.prepare(budget(), reserved.reservationId());
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
                            queries.prepare(budget(), reserved.reservationId()), (a, b) -> guard())
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
                            queries.prepare(budget(), secondReservation.reservationId()), (a, b) -> guard())
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
                            queries.prepare(budget(), secondReservation.reservationId()), (a, b) -> guard())
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
        }
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
        final var ref = new ControlRef(
                operation,
                PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()),
                0);
        final var body = new TargetQuotaGrantControlBody(request.next().scope().shard(), 500, ref, request);
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
