package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import com.nereusstream.delay.ownership.TargetSourceApplyRuntime;
import com.nereusstream.delay.ownership.WorkerSourceApplyLoop;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CancelCommandBody;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.CommandHash;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.CommandType;
import com.nereusstream.delay.protocol.ControlAuthor;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.ControlRole;
import com.nereusstream.delay.protocol.ControlRoleSet;
import com.nereusstream.delay.protocol.ControlTargetKind;
import com.nereusstream.delay.protocol.ControlTargetRef;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.MessagePrecondition;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.RescheduleCommandBody;
import com.nereusstream.delay.protocol.SelfRoutingId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetDomainState;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaGrant;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlBody;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
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

/** Signed bootstrap/grants and real accounting; initial Schedule and external control/lease providers are fixtures. */
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
            final var targetRequest = new TargetQuotaGrantControlRequest(
                    new TargetQuotaGrant(
                            scope.forTarget(initial.target()),
                            bytes(32, 0x51),
                            1,
                            originalGrant.accounting(),
                            originalGrant.limit(),
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
            final var scheduleAt = source(grantAt, grantAt.offset() + 1, grantAt.brokerLogAppendTimeEpochMs() + 1);
            final var binding = new TargetScheduleBinding(
                    initial.messageId(),
                    initial.commandType(),
                    initial.canonicalBody(),
                    scheduleAt,
                    initial.target(),
                    initial.domain(),
                    allocation.identity().accountingIncarnation(),
                    initial.requiredDispatchRef(),
                    initial.offeredDispatchRef(),
                    initial.controlScopeRef(),
                    initial.membershipGrantRef(),
                    initial.nativePolicyScopeRef(),
                    initial.orderingDomain());
            final var locator = new TargetMessageLocator(
                    binding.messageId(),
                    0,
                    binding.target(),
                    binding.domain(),
                    binding.accountingIncarnation(),
                    model.locator().orderingMode(),
                    null,
                    binding.digest());
            final var workModel = model.runtime().timeline();
            final var work = new TargetTimelineWorkRef(
                    locator,
                    TimelineWorkKind.INITIAL_SCHEDULE,
                    model.deliverAtEpochMs(),
                    model.retryEligibilityAtEpochMs(),
                    scheduleAt.sourceOrderToken(),
                    1,
                    1,
                    workModel.uncertainRetryAuthority(),
                    null,
                    null,
                    true);
            final var message = new TargetMessageRecord(
                    locator,
                    1,
                    model.deliverAtEpochMs(),
                    model.expireAtEpochMs(),
                    model.retryEligibilityAtEpochMs(),
                    model.nativeDeliveryPolicy(),
                    scheduleAt,
                    model.inlinePayload(),
                    null,
                    new TargetGenerationRuntimeIndex(
                            0,
                            GenerationAggregateState.SCHEDULED,
                            CurrentSendWorkKind.TIMELINE,
                            work,
                            null,
                            null,
                            List.of(),
                            0,
                            0,
                            false,
                            1));
            final var stamp = new TargetQuotaMutation(3, scheduleAt, bytes(32, 0x61));
            final var payload = TargetQuotaPayloadOwner.scheduled(
                    binding, scope.tenantScope(), allocation.accounting(), lineage, stamp);
            final var templateQueue =
                    TargetQueueState.decode(vector("target-identity-vectors.properties", "queue.active"));
            final var slot = templateQueue.domains().getFirst();
            final var queueState = new TargetQueueState(
                    binding.target(),
                    1,
                    templateQueue.controlVersion(),
                    templateQueue.admissionState(),
                    binding.accountingIncarnation(),
                    templateQueue.nativeIndexLeadCapMs(),
                    List.of(new TargetDomainState(
                            binding.domain(),
                            slot.lifecycle(),
                            binding.offeredDispatchRef(),
                            binding.controlScopeRef(),
                            binding.nativePolicyScopeRef() == null
                                    ? slot.nativePolicyScopeRef()
                                    : binding.nativePolicyScopeRef(),
                            null,
                            null)));
            new TargetMessageStore(backend, 1, 1, 1)
                    .applyAccounted(
                            budget(),
                            reader -> new TargetMessageStore.Input(
                                    List.of(new TargetMessageStore.Transition(null, message)),
                                    List.of(),
                                    List.of(
                                            reader.replace(
                                                    ColumnFamily.META,
                                                    TargetKeyCodec.identity(physical.id()),
                                                    CanonicalTargetPartition.VALUE_TYPE,
                                                    physical.canonicalBytes()),
                                            reader.replace(
                                                    ColumnFamily.META,
                                                    TargetKeyCodec.state(binding.target()),
                                                    TargetQueueState.VALUE_TYPE,
                                                    queueState.canonicalBytes()),
                                            reader.replace(
                                                    ColumnFamily.ID,
                                                    binding.encodedKey(),
                                                    TargetScheduleBinding.VALUE_TYPE,
                                                    binding.canonicalBytes()),
                                            reader.replace(
                                                    ColumnFamily.META,
                                                    payload.key(),
                                                    TargetQuotaPayloadOwner.VALUE_TYPE,
                                                    payload.canonicalBytes()))),
                            new TargetSourceAccounting(scope, lineage, scheduleAt, stamp.mutationDigest(), 16, 1, 1),
                            (a, b, c) -> guard());

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
            final var failed = firstStore.prepareFirst(budget(), command, at, policy, (reader, bound, source) -> false);
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
                                            assertArrayEquals(binding.canonicalBytes(), bound.canonicalBytes());
                                            return false;
                                        },
                                        (a, b, c) -> guard());
                            }),
                    new TargetSourceApplyRuntime.Limits(4096, 32L << 20, 60_000_000_000L, 16, 1),
                    System::nanoTime);
            final var entries = new java.util.ArrayDeque<SourceRecordConsumer.PolledSourceRecord>();
            final SourceRecordConsumer consumer = () -> java.util.Optional.ofNullable(entries.poll());
            final var loop = new WorkerSourceApplyLoop(consumer, workClasses(), runtime);
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
        }
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

    private static PreparedCommand reschedule(DelayMessageId message, KafkaSourcePosition at, int unique) {
        final var id = cancel(message, at, unique).commandId();
        final var body = new RescheduleCommandBody(
                message,
                at.brokerLogAppendTimeEpochMs() + 1000,
                new MessagePrecondition(null, null),
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
        return loop.runTurn(new SchedulerBudget(1, 1_000_000, 1_000), () -> 100);
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
