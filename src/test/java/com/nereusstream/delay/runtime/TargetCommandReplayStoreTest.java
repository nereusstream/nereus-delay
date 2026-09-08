package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.CommandCodec;
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
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaGrant;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlBody;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Retained Command evidence is explicitly seeded; first Cancel/Schedule semantics are not certified by this test. */
class TargetCommandReplayStoreTest {
    @TempDir
    Path root;

    @Test
    void workerReplaysCommandsAndConflictsWithoutChangingFirstResults() throws Exception {
        final var template = TargetQuotaGrantActivation.decode(raw("target.initial.activation"));
        final var originalGrant = template.grant();
        final var scope = originalGrant.scope().shardScope();
        final var origin = (KafkaSourcePosition) template.mutation().source();
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
            final var owner = initialized.root();
            final var source = source(origin, origin.offset() + 1, origin.brokerLogAppendTimeEpochMs() + 1);
            final var command = PreparedCommand.cancel(
                    scope.shard(),
                    DelayMessageId.random(scope.shard()),
                    new MessagePrecondition(null, null),
                    source.brokerLogAppendTimeEpochMs() + 1000);
            final var stamp = new TargetQuotaMutation(2, source, Bytes.sha256(CommandCodec.encodeFrame(command)));
            final var original =
                    new CommandResult(ApplyStatus.APPLIED, StableCode.NOT_FOUND, -1, 0, null, source.canonicalBytes());
            final var first = TargetResultRecord.command(
                    owner,
                    command.commandId(),
                    command.protocolTuple(),
                    command.commandHash(),
                    original,
                    stamp,
                    (a, b) -> {});
            final var query = TargetResultRecord.result(first, (a, b) -> {});
            final var physical = TargetResultRecord.position(owner, first, stamp, (a, b) -> {});
            backend.commit(
                    backend.prepare(budget(), reader -> new TargetSourceAccounting(
                                    scope, owner.recoveryLineage(), source, stamp.mutationDigest(), 2, 1, 1)
                            .assemble(
                                    reader,
                                    List.of(
                                            reader.replace(
                                                    ColumnFamily.DEDUPE,
                                                    first.key(),
                                                    TargetResultRecord.VALUE_TYPE,
                                                    first.canonicalBytes()),
                                            reader.replace(
                                                    ColumnFamily.DEDUPE,
                                                    query.key(),
                                                    TargetResultRecord.VALUE_TYPE,
                                                    query.canonicalBytes()),
                                            reader.replace(
                                                    ColumnFamily.DEDUPE,
                                                    physical.key(),
                                                    TargetResultRecord.VALUE_TYPE,
                                                    physical.canonicalBytes())))),
                    (a, b, c) -> guard());
            final var assignment = new SourceAssignment(
                    scope.shard(),
                    bytes(32, 0x43),
                    1,
                    new KafkaActivationBarrier(
                            scope.shard(), source.authenticatedClusterId(), source.nativeTopicUuid(), source.offset()));
            final var leases = new OxiaOwnerLeaseStore(new InMemoryOwnerLeaseStore());
            final var active = leases.transition(
                            leases.acquire(assignment, "command-worker", bytes(32, 0x44), 1, 10000)
                                    .orElseThrow(),
                            ShardLifecycleState.ACTIVE_FOR_COMMANDS)
                    .orElseThrow();
            store.recordOpenedOwnerEpoch(active.ownerEpoch());
            final var runtime = new TargetSourceApplyRuntime(
                    initialized,
                    store,
                    assignment,
                    active,
                    new TargetSourceApplyRuntime.Authorities(
                            leases,
                            SourceReplaySuccessor.strictKafka(),
                            entry -> {
                                throw new AssertionError("Command replay resolved grant authority");
                            },
                            (a, b, c) -> guard(),
                            (a, b) -> guard()),
                    new TargetSourceApplyRuntime.Limits(2048, 32L << 20, 60_000_000_000L, 16, 1),
                    System::nanoTime);
            final var queue = new java.util.ArrayDeque<SourceRecordConsumer.PolledSourceRecord>();
            final SourceRecordConsumer consumer = () -> java.util.Optional.ofNullable(queue.poll());
            final var loop = new WorkerSourceApplyLoop(consumer, workClasses(), runtime);
            final long beforeReplay = store.latestSequenceNumber();
            assertEquals(
                    original,
                    apply(loop, queue, command, source).appliedOutcome().commandResult());
            assertEquals(beforeReplay, store.latestSequenceNumber());
            final var aggregateKey = TargetQuotaAggregate.genesis(
                            scope.shard(), owner.identity().accountingIncarnation())
                    .key();
            final var before = TargetQuotaAggregate.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.META, aggregateKey), TargetQuotaAggregate.VALUE_TYPE)
                    .payload());
            final var later = source(source, source.offset() + 1, source.brokerLogAppendTimeEpochMs() + 1);
            final var duplicate =
                    apply(loop, queue, command, later).appliedOutcome().commandResult();
            assertEquals(StableCode.NOT_FOUND, duplicate.stableCode());
            assertArrayEquals(later.canonicalBytes(), duplicate.appliedSourcePosition());
            final var position = TargetResultRecord.decode(TargetValueEnvelope.decode(
                            store.get(
                                    ColumnFamily.DEDUPE,
                                    Bytes.concat(
                                            new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, 1},
                                            later.canonicalBytes())),
                            TargetResultRecord.VALUE_TYPE)
                    .payload());
            position.requireFirst(first);
            final var after = TargetQuotaAggregate.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.META, aggregateKey), TargetQuotaAggregate.VALUE_TYPE)
                    .payload());
            assertEquals(
                    before.usage().resources().add(position.recordCharge()),
                    after.usage().resources());
            final var conflict = PreparedCommand.cancel(
                    scope.shard(),
                    command.commandId(),
                    command.delayMessageId(),
                    new MessagePrecondition(1L, null),
                    command.retryUntilEpochMs());
            final var conflictAt = source(later, later.offset() + 1, later.brokerLogAppendTimeEpochMs() + 1);
            assertEquals(
                    StableCode.COMMAND_ID_CONFLICT,
                    apply(loop, queue, conflict, conflictAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            final long beforeConflictReplay = store.latestSequenceNumber();
            assertEquals(
                    StableCode.COMMAND_ID_CONFLICT,
                    apply(loop, queue, conflict, conflictAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertEquals(beforeConflictReplay, store.latestSequenceNumber());
            store.write(batch -> batch.putIngressFenceDeadline(command.retryUntilEpochMs()));
            final var fencedAt =
                    source(conflictAt, conflictAt.offset() + 1, conflictAt.brokerLogAppendTimeEpochMs() + 1);
            assertEquals(
                    StableCode.COMMAND_RETRY_WINDOW_EXPIRED,
                    apply(loop, queue, command, fencedAt)
                            .appliedOutcome()
                            .commandResult()
                            .stableCode());
            assertArrayEquals(
                    first.canonicalBytes(),
                    TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.DEDUPE, first.key()), TargetResultRecord.VALUE_TYPE)
                            .payload());
            assertArrayEquals(
                    query.canonicalBytes(),
                    TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.DEDUPE, query.key()), TargetResultRecord.VALUE_TYPE)
                            .payload());
            final long beforeSubstitution = store.latestSequenceNumber();
            assertEquals(
                    SourceApplyCoordinator.TurnStatus.APPLY_FAILURE,
                    apply(loop, queue, conflict, fencedAt).status());
            assertEquals(beforeSubstitution, store.latestSequenceNumber());
            assertEquals(true, runtime.fenced());
            assertEquals(true, loop.pendingEntry().isPresent());
        }
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
        try (var stream = TargetCommandReplayStoreTest.class.getResourceAsStream(
                "/ndip3/target-quota-grant-vectors.properties")) {
            values.load(stream);
        }
        return HexFormat.of().parseHex(values.getProperty(key));
    }
}
