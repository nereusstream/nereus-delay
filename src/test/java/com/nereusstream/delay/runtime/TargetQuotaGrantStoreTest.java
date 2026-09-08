package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.ownership.InMemoryControlTargetRegistrationAuthority;
import com.nereusstream.delay.ownership.InMemoryOwnerLeaseStore;
import com.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import com.nereusstream.delay.ownership.ShardLifecycleState;
import com.nereusstream.delay.ownership.SourceAcknowledgement;
import com.nereusstream.delay.ownership.SourceApplyCoordinator;
import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.ownership.SourceRecordConsumer;
import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.ownership.SourceReplaySuccessor;
import com.nereusstream.delay.ownership.TargetSourceApplyRuntime;
import com.nereusstream.delay.ownership.WorkerSourceApplyLoop;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.ControlAuthor;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.ControlRole;
import com.nereusstream.delay.protocol.ControlRoleSet;
import com.nereusstream.delay.protocol.ControlTargetKind;
import com.nereusstream.delay.protocol.ControlTargetRef;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
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

/** Signed source/Store development check; physical capacity and live CommitGuard remain test substitutes. */
class TargetQuotaGrantStoreTest {
    @TempDir
    Path root;

    @Test
    void signedFirstGrantCommitsAllocationResultAndPositionWhileExternalFailureMakesNoWrite() throws Exception {
        final var template = TargetQuotaGrantActivation.decode(raw("target.initial.activation"));
        final var request = template.request();
        final var origin = (KafkaSourcePosition) template.mutation().source();
        final var earlier = source(origin, origin.offset() - 1, origin.brokerLogAppendTimeEpochMs() - 1);
        final byte[] lineage = bytes(16, 0xcc);
        final var scope = request.next().scope().shardScope();
        final var grant = request.next();
        final long[] rootResources = grant.limit().resources().amounts();
        Arrays.fill(rootResources, 0, 15, 1L << 30);
        Arrays.fill(rootResources, 50, 55, 1L << 30);
        final var rootLimit = new TargetQuotaUsage(new CapacityVector(rootResources), 64, 64, 64, 64);
        final var rootRequest = new TargetQuotaGrantControlRequest(
                new TargetQuotaGrant(
                        scope,
                        bytes(32, 0x71),
                        1,
                        grant.accounting(),
                        rootLimit,
                        grant.tenantPolicyVersion(),
                        grant.tenantPolicyHash()),
                null,
                null);
        final var keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final var actor = new ControlAuthorizationContext(
                bytes(32, 0xa1), ControlRoleSet.of(ControlRole.PLATFORM_OPERATOR), bytes(32, 0xa2));
        final var operation = signed(request, bytes(32, 0x72), actor, keys);
        final var registrations = new InMemoryControlTargetRegistrationAuthority();
        registrations.register(operation.control());
        final var rootOperation = signed(rootRequest, bytes(32, 0x70), actor, keys);
        registrations.register(rootOperation.control());
        final var rootAuthority = authority(registrations, keys, actor, earlier, rootRequest, (a, b, c, d) -> {});
        final var config = ShardStoreConfig.defaults(root);
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.openTarget(config, scope.shard(), resources)) {
            final var limits = new TargetStoreBackend.WriteLimits(64, 2 << 20);
            final long emptySequence = store.latestSequenceNumber();
            final var startFailure = new IllegalStateException("source start proof unavailable");
            assertSame(
                    startFailure,
                    assertThrows(
                            IllegalStateException.class,
                            () -> TargetStoreBootstrap.prepare(
                                    store,
                                    scope,
                                    lineage,
                                    limits,
                                    budget(),
                                    rootOperation.control(),
                                    rootOperation.mutation(),
                                    earlier,
                                    rootAuthority,
                                    (a, b, c) -> {
                                        throw startFailure;
                                    })));
            assertEquals(emptySequence, store.latestSequenceNumber());
            assertNull(store.appliedShardLogPosition());
            assertThrows(
                    com.nereusstream.delay.store.ReadIncompleteException.class,
                    () -> TargetStoreBootstrap.prepare(
                            store,
                            scope,
                            lineage,
                            limits,
                            new BoundedReadBudget(1, 32L << 20, 60_000_000_000L, System::nanoTime),
                            rootOperation.control(),
                            rootOperation.mutation(),
                            earlier,
                            rootAuthority,
                            (a, b, c) -> {}));
            assertEquals(emptySequence, store.latestSequenceNumber());
            final var bootstrap = TargetStoreBootstrap.prepare(
                    store,
                    scope,
                    lineage,
                    limits,
                    budget(),
                    rootOperation.control(),
                    rootOperation.mutation(),
                    earlier,
                    rootAuthority,
                    (metadata, owner, complete) -> {
                        assertEquals(2, metadata.storeFormatVersion());
                        assertArrayEquals(
                                earlier.canonicalBytes(),
                                owner.allocation().source().canonicalBytes());
                        assertEquals(2, complete.quota().counters().changes().size());
                    });
            final byte[] orphanKey = new byte[] {(byte) 0xff};
            store.write(batch -> batch.put(ColumnFamily.META, orphanKey, new byte[] {1}));
            final long withOrphan = store.latestSequenceNumber();
            assertThrows(
                    IllegalStateException.class, () -> TargetStoreBootstrap.commit(bootstrap, (a, b, c) -> guard()));
            assertThrows(
                    IllegalStateException.class,
                    () -> TargetStoreBootstrap.prepare(
                            store,
                            scope,
                            lineage,
                            limits,
                            budget(),
                            rootOperation.control(),
                            rootOperation.mutation(),
                            earlier,
                            rootAuthority,
                            (a, b, c) -> {}));
            assertEquals(withOrphan, store.latestSequenceNumber());
            store.write(batch -> batch.delete(ColumnFamily.META, orphanKey));
            final var ready = TargetStoreBootstrap.prepare(
                    store,
                    scope,
                    lineage,
                    limits,
                    budget(),
                    rootOperation.control(),
                    rootOperation.mutation(),
                    earlier,
                    rootAuthority,
                    (a, b, c) -> {});
            final var initialized = TargetStoreBootstrap.commit(ready, (a, b, c) -> guard());
            final var backend = initialized.backend();
            final var shard = initialized.root();
            assertEquals(ApplyStatus.APPLIED, initialized.result().applyStatus());
            assertEquals(1, store.shardMutationSequence());
            final long afterBootstrap = store.latestSequenceNumber();
            assertThrows(IllegalStateException.class, () -> TargetStoreBootstrap.commit(ready, (a, b, c) -> guard()));
            assertThrows(
                    IllegalStateException.class,
                    () -> TargetStoreBootstrap.prepare(
                            store,
                            scope,
                            lineage,
                            limits,
                            budget(),
                            rootOperation.control(),
                            rootOperation.mutation(),
                            earlier,
                            rootAuthority,
                            (a, b, c) -> {}));
            assertEquals(afterBootstrap, store.latestSequenceNumber());
            final var applier = new TargetQuotaGrantStore(backend, shard.scope(), lineage, 16, 1);
            assertEquals(
                    initialized.result(),
                    applier.commit(
                            applier.prepare(
                                    budget(),
                                    rootOperation.control(),
                                    rootOperation.mutation(),
                                    earlier,
                                    rootAuthority),
                            (a, b, c) -> {
                                throw new AssertionError("bootstrap replay wrote");
                            },
                            (a, b) -> guard()));
            assertEquals(afterBootstrap, store.latestSequenceNumber());
            final var external = new CommandResolutionException(
                    StableCode.UNAUTHORIZED_SYSTEM_MUTATION, "external capacity unavailable");
            final var unavailable = authority(registrations, keys, actor, origin, request, (a, b, c, d) -> {
                throw external;
            });
            assertSame(
                    external,
                    assertThrows(
                            CommandResolutionException.class,
                            () -> applier.prepareFirst(
                                    budget(), operation.control(), operation.mutation(), origin, unavailable)));
            assertNull(store.get(ColumnFamily.META, template.key()));
            assertNull(store.get(ColumnFamily.DEDUPE, systemKey(operation.mutation())));
            final var allowed = authority(registrations, keys, actor, origin, request, (a, b, c, d) -> {});
            final var assignment = new SourceAssignment(
                    scope.shard(),
                    bytes(32, 0x61),
                    1,
                    new KafkaActivationBarrier(
                            scope.shard(), origin.authenticatedClusterId(), origin.nativeTopicUuid(), origin.offset()));
            final var leaseStore = new InMemoryOwnerLeaseStore();
            final var leaseAuthority = new OxiaOwnerLeaseStore(leaseStore);
            final var acquiring = leaseAuthority
                    .acquire(assignment, "target-worker", bytes(32, 0x62), 1, 10000)
                    .orElseThrow();
            final var active = leaseAuthority
                    .transition(acquiring, ShardLifecycleState.ACTIVE_FOR_COMMANDS)
                    .orElseThrow();
            store.recordOpenedOwnerEpoch(active.ownerEpoch());
            final var resolutions = new java.util.concurrent.atomic.AtomicInteger();
            final var runtime = new TargetSourceApplyRuntime(
                    initialized,
                    store,
                    assignment,
                    active,
                    new TargetSourceApplyRuntime.Authorities(
                            leaseAuthority,
                            SourceReplaySuccessor.strictKafka(),
                            entry -> {
                                resolutions.incrementAndGet();
                                return new TargetSourceApplyRuntime.GrantControl(
                                        operation.control(), allowed, (a, b, c) -> guard());
                            },
                            (a, b, c) -> guard(),
                            (a, b) -> guard()),
                    new TargetSourceApplyRuntime.Limits(2048, 32L << 20, 60_000_000_000L, 16, 1),
                    System::nanoTime);
            final var acknowledgements = new java.util.concurrent.atomic.AtomicInteger();
            final var polls = new java.util.concurrent.atomic.AtomicInteger();
            final var entries = new java.util.ArrayDeque<SourceRecordConsumer.PolledSourceRecord>();
            final var nativeEntry = new SourceReplayMutation(operation.mutation(), origin, null, null);
            entries.add(new SourceRecordConsumer.PolledSourceRecord(nativeEntry, (entry, outcome) -> {
                assertArrayEquals(
                        entry.position().canonicalBytes(),
                        outcome.systemMutationResult().appliedSourcePosition());
                return acknowledgements.incrementAndGet() == 1
                        ? SourceAcknowledgement.AcknowledgementResult.unknown(null)
                        : SourceAcknowledgement.AcknowledgementResult.acked();
            }));
            final SourceRecordConsumer consumer = () -> {
                polls.incrementAndGet();
                return java.util.Optional.ofNullable(entries.poll());
            };
            final var loop = new WorkerSourceApplyLoop(consumer, workClasses(), runtime);
            assertEquals(
                    SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN,
                    loop.runTurn(new SchedulerBudget(1, 1_000_000, 1_000), () -> 100)
                            .status());
            final long writtenBeforeAckRetry = store.latestSequenceNumber();
            final var completed = loop.runTurn(new SchedulerBudget(1, 1_000_000, 1_000), () -> 100);
            assertEquals(SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED, completed.status());
            assertEquals(writtenBeforeAckRetry, store.latestSequenceNumber());
            assertEquals(1, polls.get());
            assertEquals(1, resolutions.get());
            final var result = completed.appliedOutcome().systemMutationResult();
            assertEquals(ApplyStatus.APPLIED, result.applyStatus());
            assertEquals(StableCode.OK, result.stableCode());
            final var activation = TargetQuotaGrantActivation.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.META, template.key()), TargetQuotaGrantActivation.VALUE_TYPE)
                    .payload());
            assertEquals(2, activation.mutation().sequence());
            assertNotNull(store.get(ColumnFamily.META, activation.allocation().key()));
            final var first = TargetResultRecord.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.DEDUPE, systemKey(operation.mutation())),
                            TargetResultRecord.VALUE_TYPE)
                    .payload());
            assertEquals(result, SystemMutationResult.decode(first.typedPayload()));
            assertNotNull(first.allocation());
            final var position = TargetResultRecord.decode(TargetValueEnvelope.decode(
                            store.get(
                                    ColumnFamily.DEDUPE,
                                    Bytes.concat(
                                            new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, 1},
                                            origin.canonicalBytes())),
                            TargetResultRecord.VALUE_TYPE)
                    .payload());
            position.requireFirst(first);
            assertThrows(
                    IllegalStateException.class,
                    () -> applier.prepareFirst(
                            budget(),
                            operation.control(),
                            operation.mutation(),
                            source(origin, origin.offset() + 1, origin.brokerLogAppendTimeEpochMs() + 1),
                            allowed));
            final byte[] frozenFirst = store.get(ColumnFamily.DEDUPE, systemKey(operation.mutation()));
            final byte[] frozenActivation = store.get(ColumnFamily.META, template.key());
            final long nativeBeforeReplay = store.latestSequenceNumber();
            final var same = applier.prepare(budget(), operation.control(), operation.mutation(), origin, unavailable);
            assertEquals(
                    result,
                    applier.commit(
                            same,
                            (a, b, c) -> {
                                throw new AssertionError("same position wrote");
                            },
                            (a, b) -> guard()));
            assertEquals(nativeBeforeReplay, store.latestSequenceNumber());
            assertThrows(
                    IllegalStateException.class, () -> applier.commit(same, (a, b, c) -> guard(), (a, b) -> guard()));
            final var staleRead =
                    applier.prepare(budget(), operation.control(), operation.mutation(), origin, unavailable);
            final byte[] aggregateKey = TargetQuotaAggregate.genesis(
                            shard.identity().shard(), shard.identity().accountingIncarnation())
                    .key();
            final var beforeDuplicate = TargetQuotaAggregate.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.META, aggregateKey), TargetQuotaAggregate.VALUE_TYPE)
                    .payload());
            final var later = source(origin, origin.offset() + 1, origin.brokerLogAppendTimeEpochMs() + 1);
            assertEquals(
                    result,
                    applier.commit(
                            applier.prepare(budget(), operation.control(), operation.mutation(), later, unavailable),
                            (a, b, c) -> guard(),
                            (a, b) -> {
                                throw new AssertionError("later duplicate did not write");
                            }));
            assertEquals(3, store.shardMutationSequence());
            assertThrows(
                    IllegalStateException.class,
                    () -> applier.commit(staleRead, (a, b, c) -> guard(), (a, b) -> guard()));
            final var physical = TargetResultRecord.decode(TargetValueEnvelope.decode(
                            store.get(
                                    ColumnFamily.DEDUPE,
                                    Bytes.concat(
                                            new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, 1},
                                            later.canonicalBytes())),
                            TargetResultRecord.VALUE_TYPE)
                    .payload());
            physical.requireFirst(first);
            final var afterDuplicate = TargetQuotaAggregate.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.META, aggregateKey), TargetQuotaAggregate.VALUE_TYPE)
                    .payload());
            assertEquals(
                    beforeDuplicate.usage().resources().add(physical.recordCharge()),
                    afterDuplicate.usage().resources());
            final var expiredPosition = source(origin, origin.offset() + 2, 501);
            final var expiredDuplicate = applier.commit(
                    applier.prepare(budget(), operation.control(), operation.mutation(), expiredPosition, unavailable),
                    (a, b, c) -> guard(),
                    (a, b) -> guard());
            assertEquals(ApplyStatus.REJECTED, expiredDuplicate.applyStatus());
            assertEquals(StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED, expiredDuplicate.stableCode());
            assertArrayEquals(frozenFirst, store.get(ColumnFamily.DEDUPE, systemKey(operation.mutation())));
            assertArrayEquals(frozenActivation, store.get(ColumnFamily.META, template.key()));
            final long nativeBeforeExpiredReplay = store.latestSequenceNumber();
            assertEquals(
                    expiredDuplicate,
                    applier.commit(
                            applier.prepare(
                                    budget(), operation.control(), operation.mutation(), expiredPosition, unavailable),
                            (a, b, c) -> guard(),
                            (a, b) -> guard()));
            assertEquals(nativeBeforeExpiredReplay, store.latestSequenceNumber());
            final var originalMutation = operation.mutation();
            final var resigned = SystemMutation.signed(
                    originalMutation.shardId(),
                    originalMutation.type(),
                    originalMutation.retryUntilEpochMs(),
                    originalMutation.logicalOperationIdentity(),
                    originalMutation.canonicalBody(),
                    originalMutation.authorIdentity(),
                    2,
                    keys.getPrivate());
            assertThrows(
                    IllegalStateException.class,
                    () -> applier.prepare(budget(), operation.control(), resigned, expiredPosition, unavailable));
            final var expired = signed(request, bytes(32, 0x73), actor, keys);
            registrations.register(expired.control());
            final var rejection = applier.commit(
                    applier.prepareFirst(
                            budget(),
                            expired.control(),
                            expired.mutation(),
                            source(origin, origin.offset() + 3, 502),
                            allowed),
                    (a, b, c) -> guard());
            assertEquals(ApplyStatus.REJECTED, rejection.applyStatus());
            assertEquals(StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED, rejection.stableCode());
            final var unchanged = TargetQuotaGrantActivation.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.META, template.key()), TargetQuotaGrantActivation.VALUE_TYPE)
                    .payload());
            assertEquals(activation.mutation(), unchanged.mutation());
            final var duplicateEntry = new SourceReplayMutation(
                    operation.mutation(), source(origin, origin.offset() + 4, 503), null, null);
            entries.add(new SourceRecordConsumer.PolledSourceRecord(duplicateEntry, (entry, outcome) -> {
                acknowledgements.incrementAndGet();
                assertArrayEquals(
                        entry.position().canonicalBytes(),
                        outcome.systemMutationResult().appliedSourcePosition());
                return SourceAcknowledgement.AcknowledgementResult.unknown(null);
            }));
            assertEquals(
                    SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN,
                    loop.runTurn(new SchedulerBudget(1, 1_000_000, 1_000), () -> 100)
                            .status());
            assertEquals(1, resolutions.get());
            assertEquals(2, polls.get());
            final long beforeOwnerLossRetry = store.latestSequenceNumber();
            leaseAuthority.transition(active, ShardLifecycleState.DRAINING).orElseThrow();
            assertEquals(
                    SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN,
                    loop.runTurn(new SchedulerBudget(1, 1_000_000, 1_000), () -> 100)
                            .status());
            assertEquals(3, acknowledgements.get());
            assertEquals(beforeOwnerLossRetry, store.latestSequenceNumber());
            assertSame(duplicateEntry, loop.pendingEntry().orElseThrow());
            assertEquals(true, runtime.fenced());
            assertThrows(IllegalStateException.class, loop::close);
        }
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
                TargetQuotaGrantStoreTest.class.getResourceAsStream("/ndip3/target-quota-grant-vectors.properties")) {
            values.load(stream);
        }
        return HexFormat.of().parseHex(values.getProperty(key));
    }
}
