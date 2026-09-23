package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
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
import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.ownership.SourceReplaySuccessor;
import com.nereusstream.delay.ownership.TargetSourceApplyRuntime;
import com.nereusstream.delay.ownership.WorkerSourceApplyLoop;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.CheckpointUploadState;
import com.nereusstream.delay.protocol.CompatibleControlSnapshot;
import com.nereusstream.delay.protocol.ControlAuthor;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.ControlRole;
import com.nereusstream.delay.protocol.ControlRoleSet;
import com.nereusstream.delay.protocol.ControlTargetKind;
import com.nereusstream.delay.protocol.ControlTargetRef;
import com.nereusstream.delay.protocol.EvidenceCursor;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.QuotaGrantRef;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetQuotaAccounting;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaCounter;
import com.nereusstream.delay.protocol.TargetQuotaGrant;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlBody;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import com.nereusstream.delay.protocol.TargetTimeFenceBody;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.CheckpointFileInventory;
import com.nereusstream.delay.store.CheckpointManifest;
import com.nereusstream.delay.store.CheckpointManifestLimits;
import com.nereusstream.delay.store.CheckpointUploadIntentStore;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.IngressFenceState;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.TargetCheckpointCandidateTestBridge;
import com.nereusstream.delay.store.TargetCheckpointCandidateWorkClassExecutor;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
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
            final var fenceResolutions = new java.util.concurrent.atomic.AtomicInteger();
            final TargetTimeFenceVerifier.Authority fenceAuthority = (actualScope, author, mutation, position) -> {
                assertEquals(scope.shardScope(), actualScope);
                assertArrayEquals(AuthorIdentity.fence(bytes(32, 0x71), 1).canonicalBytes(), author.canonicalBytes());
                assertEquals(origin.nativeTopicUuid(), ((KafkaSourcePosition) position).nativeTopicUuid());
                return new TargetTimeFenceVerifier.Authorization(
                        keys.getPublic(), 10, 5, (ignoredScope, ignoredAuthor, ignoredSource, proof) -> true);
            };
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
                            entry -> {
                                fenceResolutions.incrementAndGet();
                                return new TargetSourceApplyRuntime.FenceControl(fenceAuthority, (a, b, c) -> guard());
                            },
                            entry -> {
                                throw new AssertionError("unexpected first Target Close authority");
                            },
                            (a, b, c) -> guard(),
                            (a, b) -> guard(),
                            entry -> {
                                throw new AssertionError("unexpected first Command authority");
                            }),
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
            final var fence = fence(scope.shard(), 600, keys);
            final var fenceAt = source(origin, origin.offset() + 4, 503);
            final var fenceStore =
                    new TargetTimeFenceStore(initialized.backend(), scope.shardScope(), shard.recoveryLineage(), 16, 1);
            final long nativeBeforeFence = store.latestSequenceNumber();
            final var incomplete = assertThrows(
                    com.nereusstream.delay.store.ReadIncompleteException.class,
                    () -> fenceStore.prepareFirst(
                            new BoundedReadBudget(1, 1_000_000, 60_000_000_000L, () -> 0),
                            fence,
                            fenceAt,
                            fenceAuthority));
            final var authorityFailure = assertThrows(
                    IllegalStateException.class,
                    () -> fenceStore.prepareFirst(budget(), fence, fenceAt, (a, b, c, d) -> {
                        throw incomplete;
                    }));
            assertSame(incomplete, authorityFailure.getCause());
            assertEquals(nativeBeforeFence, store.latestSequenceNumber());
            final var failedFence = fenceStore.prepareFirst(budget(), fence, fenceAt, fenceAuthority);
            assertThrows(
                    IllegalStateException.class,
                    () -> fenceStore.commit(failedFence, (a, b, c) -> {
                        throw new IllegalStateException("fence capacity unavailable");
                    }));
            assertEquals(nativeBeforeFence, store.latestSequenceNumber());
            assertNull(store.get(ColumnFamily.META, KeyCodec.metaFixed(4)));
            assertNull(store.get(ColumnFamily.DEDUPE, systemKey(fence)));
            final var shortBackend = new TargetStoreBackend(
                    store,
                    scope.shardScope(),
                    shard.identity().accountingIncarnation(),
                    shard.recoveryLineage(),
                    new TargetStoreBackend.WriteLimits(7, 2 << 20));
            assertThrows(IllegalStateException.class, () -> new TargetTimeFenceStore(
                            shortBackend, scope.shardScope(), shard.recoveryLineage(), 16, 1)
                    .prepareFirst(budget(), fence, fenceAt, fenceAuthority));
            assertEquals(nativeBeforeFence, store.latestSequenceNumber());
            final var priorFenceUsage = TargetQuotaAggregate.decode(TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.META, aggregateKey), TargetQuotaAggregate.VALUE_TYPE)
                            .payload())
                    .usage();
            final var fenceAcks = new java.util.concurrent.atomic.AtomicInteger();
            entries.add(new SourceRecordConsumer.PolledSourceRecord(
                    new SourceReplayMutation(fence, fenceAt, null, null),
                    (entry, outcome) -> fenceAcks.incrementAndGet() == 1
                            ? SourceAcknowledgement.AcknowledgementResult.unknown(null)
                            : SourceAcknowledgement.AcknowledgementResult.acked()));
            assertEquals(
                    SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN,
                    loop.runTurn(new SchedulerBudget(1, 1_000_000, 1_000), () -> 100)
                            .status());
            final long afterFenceWrite = store.latestSequenceNumber();
            assertEquals(8, afterFenceWrite - nativeBeforeFence);
            final var fenceComplete = loop.runTurn(new SchedulerBudget(1, 1_000_000, 1_000), () -> 100);
            assertEquals(SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED, fenceComplete.status());
            assertEquals(
                    StableCode.OK,
                    fenceComplete.appliedOutcome().systemMutationResult().stableCode());
            assertEquals(afterFenceWrite, store.latestSequenceNumber());
            assertEquals(1, fenceResolutions.get());
            assertEquals(2, polls.get());
            final var fenceBody = TargetTimeFenceBody.decode(fence.canonicalBody());
            final var fenceState = IngressFenceState.decode(
                    TargetValueEnvelope.decode(store.get(ColumnFamily.META, KeyCodec.metaFixed(4)), 1)
                            .payload());
            assertEquals(600, fenceState.closedThroughEpochMs());
            assertArrayEquals(fenceBody.proofId(), fenceState.proofId());
            assertArrayEquals(fenceBody.proofId(), store.runtimeMetadata().lastIngressFenceProofId());
            assertEquals(fenceAt, store.appliedShardLogPosition());
            final var fenceFirst = resultRecord(store, systemKey(fence));
            final var fencePosition = resultRecord(
                    store, Bytes.concat(new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, 1}, fenceAt.canonicalBytes()));
            fencePosition.requireFirst(fenceFirst);
            final var fixedCharge = shard.accounting()
                    .recordCharge(
                            TargetQuotaAccounting.RecordClass.STATE,
                            KeyCodec.metaFixed(4).length,
                            fenceState.canonicalBytes().length);
            final var afterFenceUsage = TargetQuotaAggregate.decode(TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.META, aggregateKey), TargetQuotaAggregate.VALUE_TYPE)
                            .payload())
                    .usage();
            assertEquals(
                    priorFenceUsage
                            .resources()
                            .add(fenceFirst.recordCharge())
                            .add(fencePosition.recordCharge())
                            .add(fixedCharge),
                    afterFenceUsage.resources());
            final var replayFence = new TargetSystemReplayStore(
                    initialized.backend(), scope.shardScope(), shard.recoveryLineage(), 16, 1);
            replayFence.commit(
                    replayFence.prepareIfPresent(budget(), fence, fenceAt).orElseThrow(),
                    (a, b, c) -> {
                        throw new AssertionError("physical fence replay wrote");
                    },
                    (a, b) -> guard());
            assertEquals(afterFenceWrite, store.latestSequenceNumber());
            // A later lower fence records its proof but cannot regress the persisted closed-through watermark.
            final var lower = fence(scope.shard(), 550, keys);
            final var lowerAt = source(origin, origin.offset() + 5, 504);
            final var lowerResult = applyMutation(loop, entries, lower, lowerAt);
            assertEquals(StableCode.OK, lowerResult.stableCode());
            final byte[] retainedFence = store.get(ColumnFamily.META, KeyCodec.metaFixed(4));
            final var lowerState = IngressFenceState.decode(
                    TargetValueEnvelope.decode(retainedFence, 1).payload());
            assertEquals(600, lowerState.closedThroughEpochMs());
            assertArrayEquals(TargetTimeFenceBody.decode(lower.canonicalBody()).proofId(), lowerState.proofId());
            assertArrayEquals(lowerState.proofId(), store.runtimeMetadata().lastIngressFenceProofId());
            assertEquals(2, fenceResolutions.get());
            final var lowerFirst = resultRecord(store, systemKey(lower));
            final var lowerPosition = resultRecord(
                    store, Bytes.concat(new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, 1}, lowerAt.canonicalBytes()));
            final var lowerUsage = TargetQuotaAggregate.decode(TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.META, aggregateKey), TargetQuotaAggregate.VALUE_TYPE)
                            .payload())
                    .usage();
            assertEquals(
                    afterFenceUsage.resources().add(lowerFirst.recordCharge()).add(lowerPosition.recordCharge()),
                    lowerUsage.resources());
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new TargetStoreBackend.IngressFenceChange(
                            retainedFence, new IngressFenceState(599, lowerState.proofId())));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new TargetStoreBackend.Edit(
                            ColumnFamily.META, KeyCodec.metaFixed(4), retainedFence, retainedFence));
            applyMutation(loop, entries, fence, source(origin, origin.offset() + 6, 505));
            assertEquals(2, fenceResolutions.get());
            assertArrayEquals(retainedFence, store.get(ColumnFamily.META, KeyCodec.metaFixed(4)));
            final var wrongFence = fence(
                    scope.shard(), 650, KeyPairGenerator.getInstance("Ed25519").generateKeyPair());
            assertEquals(
                    StableCode.UNAUTHORIZED_SYSTEM_MUTATION,
                    applyMutation(loop, entries, wrongFence, source(origin, origin.offset() + 7, 506))
                            .stableCode());
            assertArrayEquals(retainedFence, store.get(ColumnFamily.META, KeyCodec.metaFixed(4)));
            assertEquals(3, fenceResolutions.get());
            final var duplicateEntry = new SourceReplayMutation(
                    operation.mutation(), source(origin, origin.offset() + 8, 507), null, null);
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
            assertEquals(6, polls.get());
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
        final Path physicalDb;
        final byte[] checkpointId = bytes(16, 0x36);
        final List<EvidenceCursor> cursors;
        final long openedOwnerEpoch;
        try (var resources = new SharedRocksDbResources(config);
                var reopened = ShardStore.openTarget(config, scope.shard(), resources)) {
            physicalDb = reopened.dbPath();
            final var state = IngressFenceState.decode(
                    TargetValueEnvelope.decode(reopened.get(ColumnFamily.META, KeyCodec.metaFixed(4)), 1)
                            .payload());
            assertEquals(600, state.closedThroughEpochMs());
            assertArrayEquals(
                    TargetTimeFenceBody.decode(fence(scope.shard(), 550, keys).canonicalBody())
                            .proofId(),
                    state.proofId());
            assertArrayEquals(state.proofId(), reopened.runtimeMetadata().lastIngressFenceProofId());
            assertEquals(source(origin, origin.offset() + 8, 507), reopened.appliedShardLogPosition());
            cursors = reopened.runtimeMetadata().evidenceCursors();
            openedOwnerEpoch = reopened.runtimeMetadata().lastOpenedOwnerEpoch();
            reopened.recordLastCheckpointId(checkpointId);
        }
        final var imageLimits = new CheckpointManifestLimits(100, 64L << 20, 64L << 20, 1024, 1 << 20, 100, 1024);
        final var rootProof = TargetCheckpointRootVerifier.validate(physicalDb, scope.shard(), imageLimits);
        assertEquals(2, rootProof.metadata().storeFormatVersion());
        assertEquals(source(origin, origin.offset() + 8, 507), rootProof.source());
        assertEquals(
                rootProof.mutationSequence(), rootProof.aggregate().mutation().sequence());
        assertArrayEquals(lineage, rootProof.root().recoveryLineage());
        final var quotaAuditLimits = new TargetCheckpointRootVerifier.QuotaAuditLimits(1_000, 8L << 20);
        assertEquals(
                rootProof.aggregate().usage(),
                TargetCheckpointRootVerifier.auditQuotaProjections(
                                physicalDb, scope.shard(), imageLimits, quotaAuditLimits)
                        .aggregate()
                        .usage());
        final var ledgerAuditLimits =
                new TargetCheckpointRootVerifier.LedgerAuditLimits(10_000, 64L << 20, 100_000, 64L << 20);
        TargetCheckpointRootVerifier.auditIndependentLedger(
                physicalDb, scope.shard(), imageLimits, quotaAuditLimits, ledgerAuditLimits);
        final Path failedCandidate = root.resolve("target-candidate-over-budget");
        final Path unboundedCandidate = root.resolve("target-candidate-unbounded");
        final Path candidate = root.resolve("target-candidate");
        try (var resources = new SharedRocksDbResources(config);
                var live = ShardStore.openTarget(config, scope.shard(), resources)) {
            final var candidateAssignment = new SourceAssignment(
                    scope.shard(),
                    bytes(32, 0x61),
                    1,
                    new KafkaActivationBarrier(
                            scope.shard(), origin.authenticatedClusterId(), origin.nativeTopicUuid(), origin.offset()));
            final var candidateLeases = new OxiaOwnerLeaseStore(new InMemoryOwnerLeaseStore());
            final var candidateAcquiring = candidateLeases
                    .acquire(candidateAssignment, "target-worker", bytes(32, 0x62), 1, 10_000)
                    .orElseThrow();
            final var candidateOwner = candidateLeases
                    .transition(candidateAcquiring, ShardLifecycleState.ACTIVE_FOR_COMMANDS)
                    .orElseThrow();
            assertEquals(openedOwnerEpoch, candidateOwner.ownerEpoch());
            final var pending = new CheckpointUploadIntent(
                    new ShardSubject(scope.shard()),
                    lineage,
                    checkpointId,
                    new OwnerIdentity(bytes(8, 0x40), bytes(8, 0x41), openedOwnerEpoch, candidateOwner.leaseToken()),
                    live.metadata().storeIncarnation(),
                    bytes(32, 0x42),
                    1,
                    null,
                    null,
                    new ProfileRef(bytes(8, 0x43), 1, bytes(32, 0x44), ProfileKind.OBJECT_STORE),
                    new TrustedUtcIntervalEvidence(
                            1,
                            2,
                            TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                            bytes(32, 0x45),
                            1,
                            1,
                            1,
                            bytes(32, 0x46),
                            0,
                            null),
                    5_000,
                    CheckpointUploadState.PENDING_UPLOAD,
                    1,
                    null,
                    null);
            final var candidateIntents = new CheckpointUploadIntentStore();
            candidateIntents.create(pending);
            final var candidateClasses = workClasses();
            final var candidateExecutor = new TargetCheckpointCandidateWorkClassExecutor(
                    candidateClasses, live, candidateLeases, candidateIntents, () -> 100);
            final var candidateRequest = new TargetCheckpointCandidateWorkClassExecutor.Request(
                    candidate, pending, candidateOwner, imageLimits, quotaAuditLimits, ledgerAuditLimits);
            final long beforeInvalidLimits = live.operationStatistics().nativeWriteCalls();
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetCheckpointCandidateTestBridge.create(
                            live,
                            unboundedCandidate,
                            bytes(16, 0x38),
                            lineage,
                            CheckpointManifestLimits.unbounded(),
                            quotaAuditLimits,
                            ledgerAuditLimits));
            assertEquals(beforeInvalidLimits, live.operationStatistics().nativeWriteCalls());
            assertTrue(Files.notExists(unboundedCandidate));
            assertTrue(assertThrows(
                            IllegalStateException.class,
                            () -> TargetCheckpointCandidateTestBridge.create(
                                    live,
                                    failedCandidate,
                                    bytes(16, 0x37),
                                    lineage,
                                    imageLimits,
                                    quotaAuditLimits,
                                    new TargetCheckpointRootVerifier.LedgerAuditLimits(1, 128, 100_000, 64L << 20)))
                    .getCause()
                    .getMessage()
                    .contains("ledger scan budget"));
            assertTrue(Files.notExists(failedCandidate));
            assertArrayEquals(checkpointId, live.runtimeMetadata().lastCheckpointId());
            final long beforeQueue = live.operationStatistics().nativeWriteCalls();
            final var queuedCandidate = candidateExecutor.submit(candidateRequest);
            assertTrue(Files.notExists(candidate));
            assertEquals(beforeQueue, live.operationStatistics().nativeWriteCalls());
            candidateClasses.runTurn(new SchedulerBudget(1, queuedCandidate.task().bytes(), 1_000));
            assertEquals(candidate, queuedCandidate.outcome().orElseThrow().checkpointPath());
            final long beforeReuse = live.operationStatistics().nativeWriteCalls();
            final var queuedReuse = candidateExecutor.submit(candidateRequest);
            candidateClasses.runTurn(new SchedulerBudget(1, queuedReuse.task().bytes(), 1_000));
            assertEquals(candidate, queuedReuse.outcome().orElseThrow().checkpointPath());
            assertEquals(beforeReuse, live.operationStatistics().nativeWriteCalls());
            final var staleIntents = new CheckpointUploadIntentStore();
            staleIntents.create(pending);
            final var staleIntentExecutor = new TargetCheckpointCandidateWorkClassExecutor(
                    candidateClasses, live, candidateLeases, staleIntents, () -> 100);
            final Path lostIntentCandidate = root.resolve("target-candidate-lost-intent");
            final var queuedAfterIntentLoss = staleIntentExecutor.submit(
                    new TargetCheckpointCandidateWorkClassExecutor.Request(
                            lostIntentCandidate,
                            pending,
                            candidateOwner,
                            imageLimits,
                            quotaAuditLimits,
                            ledgerAuditLimits));
            staleIntents.beginReaping(
                    pending,
                    new TrustedUtcIntervalEvidence(
                            5_000,
                            5_001,
                            TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                            bytes(32, 0x47),
                            1,
                            2,
                            2,
                            bytes(32, 0x48),
                            0,
                            null));
            candidateClasses.runTurn(new SchedulerBudget(1, queuedAfterIntentLoss.task().bytes(), 1_000));
            assertTrue(queuedAfterIntentLoss.outcome().orElseThrow().failure() instanceof IllegalStateException);
            assertTrue(Files.notExists(lostIntentCandidate));
            assertEquals(beforeReuse, live.operationStatistics().nativeWriteCalls());
            final Path lostOwnerCandidate = root.resolve("target-candidate-lost-owner");
            final var queuedAfterOwnerLoss = candidateExecutor.submit(
                    new TargetCheckpointCandidateWorkClassExecutor.Request(
                            lostOwnerCandidate,
                            pending,
                            candidateOwner,
                            imageLimits,
                            quotaAuditLimits,
                            ledgerAuditLimits));
            candidateLeases.transition(candidateOwner, ShardLifecycleState.DRAINING).orElseThrow();
            final long beforeOwnerLoss = live.operationStatistics().nativeWriteCalls();
            candidateClasses.runTurn(new SchedulerBudget(1, queuedAfterOwnerLoss.task().bytes(), 1_000));
            assertTrue(queuedAfterOwnerLoss.outcome().orElseThrow().failure() instanceof IllegalStateException);
            assertTrue(Files.notExists(lostOwnerCandidate));
            assertEquals(beforeOwnerLoss, live.operationStatistics().nativeWriteCalls());
            assertThrows(
                    IllegalStateException.class,
                    () -> TargetCheckpointCandidateTestBridge.reuse(
                            live,
                            candidate,
                            bytes(16, 0x39),
                            lineage,
                            imageLimits,
                            quotaAuditLimits,
                            ledgerAuditLimits));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetCheckpointCandidateTestBridge.reuse(
                            live,
                            candidate,
                            checkpointId,
                            bytes(16, 0x39),
                            imageLimits,
                            quotaAuditLimits,
                            ledgerAuditLimits));
            assertEquals(beforeReuse, live.operationStatistics().nativeWriteCalls());
            final long candidateWriteCut = live.latestSequenceNumber();
            live.recordLastCheckpointId(checkpointId);
            assertTrue(Long.compareUnsigned(live.latestSequenceNumber(), candidateWriteCut) > 0);
            assertTrue(assertThrows(
                            IllegalArgumentException.class,
                            () -> TargetCheckpointCandidateTestBridge.reuse(
                                    live,
                                    candidate,
                                    checkpointId,
                                    lineage,
                                    imageLimits,
                                    quotaAuditLimits,
                                    ledgerAuditLimits))
                    .getMessage()
                    .contains("write cut"));
        }
        assertTrue(Files.isRegularFile(candidate.resolve("CURRENT")));
        assertEquals(
                rootProof.source(),
                TargetCheckpointRootVerifier.auditIndependentLedger(
                                candidate, scope.shard(), imageLimits, quotaAuditLimits, ledgerAuditLimits)
                        .source());
        assertTrue(assertThrows(
                        IllegalArgumentException.class,
                        () -> TargetCheckpointRootVerifier.auditIndependentLedger(
                                physicalDb,
                                scope.shard(),
                                imageLimits,
                                quotaAuditLimits,
                                new TargetCheckpointRootVerifier.LedgerAuditLimits(1, 128, 100_000, 64L << 20)))
                .getMessage()
                .contains("ledger scan budget"));
        assertTrue(assertThrows(
                        IllegalArgumentException.class,
                        () -> TargetCheckpointRootVerifier.auditIndependentLedger(
                                physicalDb,
                                scope.shard(),
                                imageLimits,
                                quotaAuditLimits,
                                new TargetCheckpointRootVerifier.LedgerAuditLimits(10_000, 64L << 20, 1, 128)))
                .getMessage()
                .contains("point-read budget"));
        final byte[] positionKey =
                Bytes.concat(new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, 1}, origin.canonicalBytes());
        final byte[] originalPosition;
        try (var resources = new SharedRocksDbResources(config);
                var corrupt = ShardStore.openTarget(config, scope.shard(), resources)) {
            originalPosition = corrupt.get(ColumnFamily.DEDUPE, positionKey);
            assertNotNull(originalPosition);
            corrupt.write(batch -> batch.delete(ColumnFamily.DEDUPE, positionKey));
        }
        TargetCheckpointRootVerifier.auditQuotaProjections(physicalDb, scope.shard(), imageLimits, quotaAuditLimits);
        assertTrue(assertThrows(
                        IllegalStateException.class,
                        () -> TargetCheckpointRootVerifier.auditIndependentLedger(
                                physicalDb, scope.shard(), imageLimits, quotaAuditLimits, ledgerAuditLimits))
                .getMessage()
                .contains("quota counter differs from independently rebuilt ledger usage"));
        try (var resources = new SharedRocksDbResources(config);
                var restored = ShardStore.openTarget(config, scope.shard(), resources)) {
            restored.write(batch -> batch.put(ColumnFamily.DEDUPE, positionKey, originalPosition));
        }
        TargetCheckpointRootVerifier.auditIndependentLedger(
                physicalDb, scope.shard(), imageLimits, quotaAuditLimits, ledgerAuditLimits);
        final byte[] foreignKey = new byte[] {0x7f, 1, 1};
        try (var resources = new SharedRocksDbResources(config);
                var corrupt = ShardStore.openTarget(config, scope.shard(), resources)) {
            corrupt.write(batch ->
                    batch.put(ColumnFamily.DEDUPE, foreignKey, TargetValueEnvelope.encode(35, bytes(4, 0x29))));
        }
        assertTrue(assertThrows(
                        IllegalArgumentException.class,
                        () -> TargetCheckpointRootVerifier.auditIndependentLedger(
                                physicalDb, scope.shard(), imageLimits, quotaAuditLimits, ledgerAuditLimits))
                .getMessage()
                .contains("unregistered or backend-owned Target key"));
        try (var resources = new SharedRocksDbResources(config);
                var restored = ShardStore.openTarget(config, scope.shard(), resources)) {
            restored.write(batch -> batch.delete(ColumnFamily.DEDUPE, foreignKey));
        }
        TargetCheckpointRootVerifier.auditIndependentLedger(
                physicalDb, scope.shard(), imageLimits, quotaAuditLimits, ledgerAuditLimits);
        final var legacyControl = new CompatibleControlSnapshot(
                new ShardSubject(scope.shard()),
                List.of(new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 1)),
                List.of(),
                new QuotaGrantRef(
                        bytes(32, 0x5a),
                        1,
                        new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)));
        try (var resources = new SharedRocksDbResources(config);
                var corrupt = ShardStore.openTarget(config, scope.shard(), resources)) {
            final long beforeRejectedControl = corrupt.latestSequenceNumber();
            assertThrows(IllegalStateException.class, () -> corrupt.recordControlSnapshot(legacyControl));
            assertEquals(beforeRejectedControl, corrupt.latestSequenceNumber());
            corrupt.write(batch -> batch.putValue(
                    ColumnFamily.META, 1, KeyCodec.metaFixed(10), legacyControl.canonicalBytes()));
        }
        assertTrue(assertThrows(
                        IllegalArgumentException.class,
                        () -> TargetCheckpointRootVerifier.auditIndependentLedger(
                                physicalDb, scope.shard(), imageLimits, quotaAuditLimits, ledgerAuditLimits))
                .getMessage()
                .contains("unknown Target checkpoint fixed metadata key"));
        try (var resources = new SharedRocksDbResources(config);
                var restored = ShardStore.openTarget(config, scope.shard(), resources)) {
            restored.write(batch -> batch.delete(ColumnFamily.META, KeyCodec.metaFixed(10)));
        }
        TargetCheckpointRootVerifier.auditIndependentLedger(
                physicalDb, scope.shard(), imageLimits, quotaAuditLimits, ledgerAuditLimits);
        assertTrue(assertThrows(
                        IllegalArgumentException.class,
                        () -> TargetCheckpointRootVerifier.auditQuotaProjections(
                                physicalDb,
                                scope.shard(),
                                imageLimits,
                                new TargetCheckpointRootVerifier.QuotaAuditLimits(1, 128)))
                .getMessage()
                .contains("quota audit budget"));
        final var files = CheckpointFileInventory.collect(physicalDb, imageLimits).stream()
                .map(file -> new CheckpointManifest.FileEntry(
                        file.name(),
                        file.length(),
                        file.checksum(),
                        Bytes.utf8("object/" + file.name()),
                        Bytes.utf8("version-1"),
                        null))
                .toList();
        final var manifest = new CheckpointManifest(
                checkpointId,
                lineage,
                0,
                null,
                null,
                new CheckpointManifest.CreatedBy(bytes(8, 0x41), bytes(8, 0x42), openedOwnerEpoch),
                new CheckpointManifest.CreatedAt(
                        900, 1_000, "CERTIFIED_HOST_CLOCK", bytes(8, 0x43), 1, 2, 3, bytes(32, 0x44), 0, null),
                scope.shard(),
                rootProof.metadata().dbIdentity(),
                rootProof.metadata().storeIncarnationUuid(),
                2,
                rootProof.mutationSequence(),
                rootProof.source(),
                bytes(32, 0x45),
                bytes(32, 0x46),
                cursors,
                files);
        final var bound = TargetCheckpointRootVerifier.validateManifestImageIdentity(physicalDb, manifest, imageLimits);
        assertEquals(rootProof.mutationSequence(), bound.mutationSequence());
        assertEquals(rootProof.source(), bound.source());
        assertArrayEquals(rootProof.root().digest(), bound.root().digest());
        assertEquals(
                rootProof.aggregate().usage(),
                TargetCheckpointRootVerifier.auditManifestImageLedger(
                                physicalDb, manifest, imageLimits, quotaAuditLimits, ledgerAuditLimits)
                        .aggregate()
                        .usage());
        assertManifestBindingFailure(
                physicalDb,
                copyManifest(
                        manifest,
                        bytes(16, 0x56),
                        manifest.dbIdentity(),
                        manifest.shardMutationSequence(),
                        manifest.appliedShardLogPosition(),
                        manifest.files()),
                imageLimits,
                "checkpoint identity");
        assertManifestBindingFailure(
                physicalDb,
                copyManifest(
                        manifest,
                        manifest.checkpointId(),
                        bytes(32, 0x57),
                        manifest.shardMutationSequence(),
                        manifest.appliedShardLogPosition(),
                        manifest.files()),
                imageLimits,
                "Store/source identity");
        assertManifestBindingFailure(
                physicalDb,
                copyManifest(
                        manifest,
                        manifest.checkpointId(),
                        manifest.dbIdentity(),
                        manifest.shardMutationSequence() + 1,
                        manifest.appliedShardLogPosition(),
                        manifest.files()),
                imageLimits,
                "Store/source identity");
        assertManifestBindingFailure(
                physicalDb,
                copyManifest(
                        manifest,
                        manifest.checkpointId(),
                        manifest.dbIdentity(),
                        manifest.shardMutationSequence(),
                        source(origin, origin.offset() + 9, 508),
                        manifest.files()),
                imageLimits,
                "Store/source identity");
        final var originalFile = manifest.files().getFirst();
        final var alteredFile = new CheckpointManifest.FileEntry(
                originalFile.name(),
                originalFile.length(),
                bytes(32, 0x58),
                originalFile.objectKey(),
                originalFile.objectVersion(),
                originalFile.etag());
        final var alteredFiles = new java.util.ArrayList<>(manifest.files());
        alteredFiles.set(0, alteredFile);
        assertManifestBindingFailure(
                physicalDb,
                copyManifest(
                        manifest,
                        manifest.checkpointId(),
                        manifest.dbIdentity(),
                        manifest.shardMutationSequence(),
                        manifest.appliedShardLogPosition(),
                        alteredFiles),
                imageLimits,
                "file inventory");
        assertManifestBindingFailure(
                physicalDb,
                copyManifestAuthority(
                        manifest,
                        bytes(16, 0x59),
                        manifest.sourceStoreIncarnation(),
                        manifest.evidenceCursors(),
                        manifest.createdBy()),
                imageLimits,
                "Store/source identity");
        assertManifestBindingFailure(
                physicalDb,
                copyManifestAuthority(
                        manifest,
                        manifest.recoveryLineageId(),
                        new UUID(7, 8),
                        manifest.evidenceCursors(),
                        manifest.createdBy()),
                imageLimits,
                "Store/source identity");
        assertManifestBindingFailure(
                physicalDb,
                copyManifestAuthority(
                        manifest,
                        manifest.recoveryLineageId(),
                        manifest.sourceStoreIncarnation(),
                        List.of(EvidenceCursor.kafka(bytes(32, 0x60), bytes(16, 0x61), bytes(16, 0x62), 0, 1, 1, 1, 1)),
                        manifest.createdBy()),
                imageLimits,
                "evidence cursors");
        assertManifestBindingFailure(
                physicalDb,
                copyManifestAuthority(
                        manifest,
                        manifest.recoveryLineageId(),
                        manifest.sourceStoreIncarnation(),
                        manifest.evidenceCursors(),
                        new CheckpointManifest.CreatedBy(
                                manifest.createdBy().deploymentId(),
                                manifest.createdBy().workerRunId(),
                                openedOwnerEpoch + 1)),
                imageLimits,
                "Owner epoch");
        final byte[] aggregateKey = rootProof.aggregate().key();
        final byte[] originalAggregate;
        try (var resources = new SharedRocksDbResources(config);
                var corrupt = ShardStore.openTarget(config, scope.shard(), resources)) {
            originalAggregate = corrupt.get(ColumnFamily.META, aggregateKey);
            final var inflated = new TargetQuotaAggregate(
                    scope.shard(),
                    rootProof.aggregate().accountingIncarnation(),
                    rootProof.aggregate().usage().add(rootProof.aggregate().usage()),
                    rootProof.aggregate().revision(),
                    rootProof.aggregate().mutation());
            corrupt.write(batch -> batch.put(
                    ColumnFamily.META,
                    aggregateKey,
                    TargetValueEnvelope.encode(TargetQuotaAggregate.VALUE_TYPE, inflated.canonicalBytes())));
        }
        TargetCheckpointRootVerifier.validate(physicalDb, scope.shard(), imageLimits);
        assertTrue(assertThrows(
                        IllegalStateException.class,
                        () -> TargetCheckpointRootVerifier.auditQuotaProjections(
                                physicalDb, scope.shard(), imageLimits, quotaAuditLimits))
                .getMessage()
                .contains("aggregate differs from primary counters"));
        try (var resources = new SharedRocksDbResources(config);
                var restored = ShardStore.openTarget(config, scope.shard(), resources)) {
            restored.write(batch -> batch.put(ColumnFamily.META, aggregateKey, originalAggregate));
        }
        TargetCheckpointRootVerifier.auditQuotaProjections(physicalDb, scope.shard(), imageLimits, quotaAuditLimits);
        final byte[] mirrorKey = rootProof.bookkeeping().tenantOwner().key();
        final byte[] originalMirror;
        try (var resources = new SharedRocksDbResources(config);
                var corrupt = ShardStore.openTarget(config, scope.shard(), resources)) {
            originalMirror = corrupt.get(ColumnFamily.META, mirrorKey);
            final var mirror =
                    TargetQuotaCounter.decode(TargetValueEnvelope.decode(originalMirror, TargetQuotaCounter.VALUE_TYPE)
                            .payload());
            final var inflated = new TargetQuotaCounter(
                    mirror.identity(), mirror.usage().add(mirror.usage()), mirror.revision(), mirror.mutation());
            corrupt.write(batch -> batch.put(
                    ColumnFamily.META,
                    mirrorKey,
                    TargetValueEnvelope.encode(TargetQuotaCounter.VALUE_TYPE, inflated.canonicalBytes())));
        }
        TargetCheckpointRootVerifier.validate(physicalDb, scope.shard(), imageLimits);
        assertTrue(assertThrows(
                        IllegalStateException.class,
                        () -> TargetCheckpointRootVerifier.auditQuotaProjections(
                                physicalDb, scope.shard(), imageLimits, quotaAuditLimits))
                .getMessage()
                .contains("primary and tenant mirror disagree"));
        try (var resources = new SharedRocksDbResources(config);
                var restored = ShardStore.openTarget(config, scope.shard(), resources)) {
            restored.write(batch -> batch.put(ColumnFamily.META, mirrorKey, originalMirror));
        }
        TargetCheckpointRootVerifier.auditQuotaProjections(physicalDb, scope.shard(), imageLimits, quotaAuditLimits);
        try (var resources = new SharedRocksDbResources(config);
                var corrupt = ShardStore.openTarget(config, scope.shard(), resources)) {
            corrupt.write(batch -> batch.delete(ColumnFamily.META, mirrorKey));
        }
        TargetCheckpointRootVerifier.validate(physicalDb, scope.shard(), imageLimits);
        assertTrue(assertThrows(
                        IllegalStateException.class,
                        () -> TargetCheckpointRootVerifier.auditQuotaProjections(
                                physicalDb, scope.shard(), imageLimits, quotaAuditLimits))
                .getMessage()
                .contains("bookkeeping inventory"));
        try (var resources = new SharedRocksDbResources(config);
                var corrupt = ShardStore.openTarget(config, scope.shard(), resources)) {
            corrupt.write(batch -> batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(5), Bytes.u64beBits(999)));
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetCheckpointRootVerifier.validate(physicalDb, scope.shard(), imageLimits));
        try (var resources = new SharedRocksDbResources(config);
                var corrupt = ShardStore.openTarget(config, scope.shard(), resources)) {
            corrupt.write(batch -> {
                batch.putValue(
                        ColumnFamily.META, 1, KeyCodec.metaFixed(5), Bytes.u64beBits(rootProof.mutationSequence()));
                batch.put(
                        ColumnFamily.META,
                        rootProof.root().key(),
                        TargetValueEnvelope.encode(TargetQuotaIncarnation.VALUE_TYPE, new byte[] {1}));
            });
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetCheckpointRootVerifier.validate(physicalDb, scope.shard(), imageLimits));
    }

    private static CheckpointManifest copyManifest(
            final CheckpointManifest base,
            final byte[] checkpointId,
            final byte[] dbIdentity,
            final long mutationSequence,
            final SourcePosition source,
            final List<CheckpointManifest.FileEntry> files) {
        return new CheckpointManifest(
                checkpointId,
                base.recoveryLineageId(),
                base.lineageGeneration(),
                base.parentCheckpoint(),
                base.restoredFromCheckpointId(),
                base.createdBy(),
                base.createdAt(),
                base.shardId(),
                dbIdentity,
                base.sourceStoreIncarnation(),
                base.storeFormatVersion(),
                mutationSequence,
                source,
                base.controlStateDigest(),
                base.referencedSemanticVersionsDigest(),
                base.evidenceCursors(),
                files);
    }

    private static CheckpointManifest copyManifestAuthority(
            final CheckpointManifest base,
            final byte[] lineage,
            final UUID incarnation,
            final List<EvidenceCursor> cursors,
            final CheckpointManifest.CreatedBy createdBy) {
        return new CheckpointManifest(
                base.checkpointId(),
                lineage,
                base.lineageGeneration(),
                base.parentCheckpoint(),
                base.restoredFromCheckpointId(),
                createdBy,
                base.createdAt(),
                base.shardId(),
                base.dbIdentity(),
                incarnation,
                base.storeFormatVersion(),
                base.shardMutationSequence(),
                base.appliedShardLogPosition(),
                base.controlStateDigest(),
                base.referencedSemanticVersionsDigest(),
                cursors,
                base.files());
    }

    private static void assertManifestBindingFailure(
            final Path image,
            final CheckpointManifest manifest,
            final CheckpointManifestLimits limits,
            final String message) {
        assertTrue(assertThrows(
                        IllegalArgumentException.class,
                        () -> TargetCheckpointRootVerifier.validateManifestImageIdentity(image, manifest, limits))
                .getMessage()
                .contains(message));
    }

    private static SystemMutation fence(com.nereusstream.delay.protocol.ShardId shard, long close, KeyPair keys) {
        final var proof = new TrustedUtcIntervalEvidence(
                close + 10,
                close + 15,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                bytes(32, 0x72),
                1,
                1,
                1,
                bytes(32, 0x73),
                0,
                new byte[0]);
        final var body = new TargetTimeFenceBody(shard, 2000, close, 1, proof);
        return SystemMutation.signed(
                shard,
                SystemMutationType.TIME_FENCE,
                body.retryUntil(),
                body.proofId(),
                body.canonicalBytes(),
                AuthorIdentity.fence(bytes(32, 0x71), 1).canonicalBytes(),
                1,
                keys.getPrivate());
    }

    private static TargetResultRecord resultRecord(ShardStore store, byte[] key) {
        return TargetResultRecord.decode(
                TargetValueEnvelope.decode(store.get(ColumnFamily.DEDUPE, key), TargetResultRecord.VALUE_TYPE)
                        .payload());
    }

    private static SystemMutationResult applyMutation(
            WorkerSourceApplyLoop loop,
            java.util.ArrayDeque<SourceRecordConsumer.PolledSourceRecord> entries,
            SystemMutation mutation,
            KafkaSourcePosition source) {
        entries.add(new SourceRecordConsumer.PolledSourceRecord(
                new SourceReplayMutation(mutation, source, null, null),
                (entry, outcome) -> SourceAcknowledgement.AcknowledgementResult.acked()));
        final var turn = loop.runTurn(new SchedulerBudget(1, 1_000_000, 1_000), () -> 100);
        assertEquals(SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED, turn.status());
        return turn.appliedOutcome().systemMutationResult();
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
