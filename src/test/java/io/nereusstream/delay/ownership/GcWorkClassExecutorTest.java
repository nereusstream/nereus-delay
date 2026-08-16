package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.ResourceDeleteConfirmedBody;
import io.nereusstream.delay.protocol.ResourceKind;
import io.nereusstream.delay.protocol.ResourceRetireIntentBody;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SystemMutation;
import io.nereusstream.delay.protocol.SystemMutationType;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
import io.nereusstream.delay.runtime.ResourceDeleteConfirmedRecord;
import io.nereusstream.delay.runtime.ResourceRetireIntentRecord;
import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassPolicy;
import io.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import io.nereusstream.delay.scheduler.WorkClassTask;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.EnumMap;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GcWorkClassExecutorTest {
    @TempDir
    Path tempDir;

    @Test
    void persistsRetireAndDeleteMutationsWithoutLocalGcApply() throws Exception {
        final Fixture fixture = fixture("persisted");
        try (fixture) {
            final SystemMutation retire = retireMutation(fixture, 1);
            final SystemMutation confirmation = deleteConfirmation(fixture, retire);
            final AtomicAppender appender = new AtomicAppender(fixture);
            final GcWorkClassExecutor executor = new GcWorkClassExecutor(fixture.workClasses,
                    fixture.owned, fixture.authority, appender);

            final GcWorkClassExecutor.Submission first = executor.submit(retire, () -> 101);
            assertEquals(WorkClass.GC, first.task().workClass());
            assertEquals(retire.encodeFrame().length, first.task().bytes());
            fixture.workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));
            assertEquals(GcWorkClassExecutor.ResultKind.PERSISTED, first.result().orElseThrow().kind());
            assertArrayEquals(retire.encodeFrame(), appender.last.get().encodeFrame());
            assertNull(fixture.owned.shard().getResourceRetireIntent(ResourceKind.LOCAL_STORE,
                    ResourceRetireIntentBody.decode(retire.canonicalBody()).resource().identityHash(), 1));

            appender.next = ShardLogMutationAppender.AppendOutcome.definitelyNotPersisted();
            final GcWorkClassExecutor.Submission second = executor.submit(confirmation, () -> 101);
            fixture.workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));
            assertEquals(GcWorkClassExecutor.ResultKind.DEFINITIVELY_NOT_PERSISTED,
                    second.result().orElseThrow().kind());
            assertNull(fixture.owned.shard().getResourceDeleteConfirmation(ResourceKind.LOCAL_STORE,
                    ResourceRetireIntentBody.decode(retire.canonicalBody()).resource().identityHash(), 1));
        }
    }

    @Test
    void queueRejectionAndExpiredOwnerRetainExactGcMutation() throws Exception {
        final Fixture fixture = fixture("fencing");
        try (fixture) {
            fixture.workClasses.submit(new WorkClassTask(WorkClass.GC, "occupied", 1), () -> {
            });
            final AtomicAppender appender = new AtomicAppender(fixture);
            final GcWorkClassExecutor executor = new GcWorkClassExecutor(fixture.workClasses,
                    fixture.owned, fixture.authority, appender);
            final SystemMutation mutation = retireMutation(fixture, 2);
            assertThrows(IllegalStateException.class, () -> executor.submit(mutation, () -> 101));
            assertEquals(0, appender.calls);
            fixture.workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));

            final GcWorkClassExecutor.Submission expired = executor.submit(mutation, () -> 250);
            fixture.workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));
            assertEquals(GcWorkClassExecutor.ResultKind.UNKNOWN, expired.result().orElseThrow().kind());
            assertEquals(0, appender.calls);
            assertEquals(ShardLifecycleState.FENCED, fixture.owned.state());
        }
    }

    @Test
    void typedDeleteConfirmationHandoffRequiresReturnedSourceAfterRetire() throws Exception {
        final Fixture fixture = fixture("source-order");
        try (fixture) {
            final SystemMutation retire = retireMutation(fixture, 3);
            final ResourceRetireIntentRecord intent = retireRecord(fixture, retire);
            final SystemMutation confirmation = deleteConfirmation(fixture, retire);
            final AtomicAppender appender = new AtomicAppender(fixture);
            final GcWorkClassExecutor executor = new GcWorkClassExecutor(fixture.workClasses,
                    fixture.owned, fixture.authority, appender);

            appender.next = ShardLogMutationAppender.AppendOutcome.persisted(fixture.position(2));
            final GcWorkClassExecutor.Submission persisted = executor.submitDeleteConfirmation(confirmation,
                    intent, () -> 101);
            fixture.workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));
            assertEquals(GcWorkClassExecutor.ResultKind.PERSISTED, persisted.result().orElseThrow().kind());

            appender.next = ShardLogMutationAppender.AppendOutcome.persisted(fixture.position(1));
            final GcWorkClassExecutor.Submission regressed = executor.submitDeleteConfirmation(confirmation,
                    intent, () -> 101);
            fixture.workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));
            assertEquals(GcWorkClassExecutor.ResultKind.UNKNOWN, regressed.result().orElseThrow().kind());
            assertEquals(ShardLifecycleState.FENCED, fixture.owned.state());
        }
    }

    private Fixture fixture(final String name) throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 47);
        final UUID topic = UUID.randomUUID();
        final SourceAssignment assignment = new SourceAssignment(shard,
                Bytes.sha256(Bytes.utf8("gc-assignment-" + name)), 7,
                new KafkaActivationBarrier(shard, "gc-cluster", topic, 0));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = backend.acquire(assignment, "gc-owner",
                Bytes.sha256(Bytes.utf8("gc-session-" + name)), 100, 100).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve(name + "-store"));
        final SharedRocksDbResources resources = new SharedRocksDbResources(config);
        final ShardStore store = ShardStore.open(config, shard, resources);
        final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
        owned.markCatchingUp(authority, assignment, SourceReplaySuccessor.strictKafka(), 101);
        owned.recordCatchup(new KafkaSourcePosition(shard, "gc-cluster", topic, 0, null, 1_000));
        owned.activateForCommands(authority, 101);
        return new Fixture(shard, topic, lease, authority, owned, workClasses(1), resources, store);
    }

    private static SystemMutation retireMutation(final Fixture fixture, final int identity) throws Exception {
        final byte[] resource = localStoreResource(fixture.shard);
        final byte[] protections = protectionSet(Bytes.sha256(Bytes.utf8("gc-protection-" + identity)));
        final byte[] body = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject(fixture.shard));
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.RESOURCE_RETIRE_INTENT.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.uint32(output, 10, ResourceKind.LOCAL_STORE.wireValue());
            CanonicalProtobuf.bytes(output, 11, resource);
            CanonicalProtobuf.uint64Bits(output, 12, 1);
            CanonicalProtobuf.bytes(output, 13, protections);
        });
        final ResourceRetireIntentBody parsed = ResourceRetireIntentBody.decode(body);
        final byte[] logicalIdentity = SystemMutation.computeResourceRetireLogicalIdentity(
                parsed.resourceKind(), parsed.resource().identityHash(), parsed.expectedResourceStateVersion());
        return SystemMutation.signed(fixture.shard, SystemMutationType.RESOURCE_RETIRE_INTENT, 9_000,
                logicalIdentity, body, serviceAuthor().canonicalBytes(), 1,
                KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPrivate());
    }

    private static SystemMutation deleteConfirmation(final Fixture fixture, final SystemMutation retire) {
        final ResourceRetireIntentBody parsed = ResourceRetireIntentBody.decode(retire.canonicalBody());
        final TrustedUtcIntervalEvidence time = evidence();
        final TrustedUtcIntervalEvidence confirmedAt = new TrustedUtcIntervalEvidence(2_002, 2_003,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("gc-confirmed-clock"),
                1, 2, 1, Bytes.sha256(Bytes.utf8("gc-confirmed-time")), 0, null);
        final byte[] intent = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, retire.systemMutationId());
            CanonicalProtobuf.bytes(output, 2, retire.mutationHash());
            CanonicalProtobuf.bytes(output, 3, parsed.resource().identityHash());
            CanonicalProtobuf.uint64Bits(output, 4, parsed.expectedResourceStateVersion());
        });
        final byte[] evidence = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, parsed.resource().identityHash());
            CanonicalProtobuf.bytes(output, 2, Bytes.sha256(Bytes.utf8("gc-provider-request")));
            CanonicalProtobuf.uint32(output, 3, ResourceDeleteConfirmedBody.DeleteOutcome.ALREADY_ABSENT.wireValue());
            CanonicalProtobuf.bytes(output, 6, Bytes.sha256(Bytes.utf8("gc-delete-response")));
            CanonicalProtobuf.bytes(output, 7, time.canonicalBytes());
        });
        final byte[] body = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject(fixture.shard));
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.RESOURCE_DELETE_CONFIRMED.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, intent);
            CanonicalProtobuf.uint32(output, 11, ResourceDeleteConfirmedBody.DeleteOutcome.ALREADY_ABSENT.wireValue());
            CanonicalProtobuf.bytes(output, 12, evidence);
            CanonicalProtobuf.bytes(output, 13, confirmedAt.canonicalBytes());
        });
        return SystemMutation.signed(fixture.shard, SystemMutationType.RESOURCE_DELETE_CONFIRMED, 9_000,
                retire.systemMutationId(), body, serviceAuthor().canonicalBytes(), 1,
                KeyPairGeneratorHolder.KEY_PAIR.getPrivate());
    }

    private static ResourceRetireIntentRecord retireRecord(final Fixture fixture, final SystemMutation retire) {
        final ResourceRetireIntentBody parsed = ResourceRetireIntentBody.decode(retire.canonicalBody());
        return new ResourceRetireIntentRecord(retire.systemMutationId(), retire.mutationHash(),
                parsed.resourceKind(), parsed.resource().canonicalBytes(), parsed.resource().identityHash(),
                parsed.expectedResourceStateVersion(), 1, parsed.protections().canonicalBytes(),
                fixture.position(1).canonicalBytes());
    }

    private static final class KeyPairGeneratorHolder {
        private static final KeyPair KEY_PAIR;

        static {
            try {
                KEY_PAIR = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            } catch (Exception failure) {
                throw new ExceptionInInitializerError(failure);
            }
        }
    }

    private static AuthorIdentity serviceAuthor() {
        return AuthorIdentity.service(Bytes.utf8("gc-service"), Bytes.utf8("gc-run"), 1);
    }

    private static byte[] subject(final ShardId shard) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
    }

    private static byte[] localStoreResource(final ShardId shard) {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, ResourceKind.LOCAL_STORE.wireValue(),
                CanonicalProtobuf.message(local -> {
                    CanonicalProtobuf.bytes(local, 1, subject(shard));
                    CanonicalProtobuf.bytes(local, 2, new byte[16]);
                    CanonicalProtobuf.bytes(local, 3, Bytes.sha256(Bytes.utf8("gc-db-identity")));
                    CanonicalProtobuf.bytes(local, 4, Bytes.sha256(Bytes.utf8("gc-root-policy")));
                })));
    }

    private static byte[] protectionSet(final byte[] protectedResourceId) {
        final byte[] reference = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 3);
            CanonicalProtobuf.bytes(output, 2, protectedResourceId);
            CanonicalProtobuf.uint32(output, 3, 1);
        });
        final byte[] repeated = CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, reference));
        final byte[] digest = Bytes.sha256(Bytes.utf8("nereus-delay-protection-set-v1\0"), repeated);
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, reference);
            CanonicalProtobuf.bytes(output, 2, digest);
        });
    }

    private static TrustedUtcIntervalEvidence evidence() {
        return new TrustedUtcIntervalEvidence(2_000, 2_001,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("gc-clock"),
                1, 1, 1, Bytes.sha256(Bytes.utf8("gc-time")), 0, null);
    }

    private static WorkClassExecutionRegistry workClasses(final int maxQueueRecords) {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass = switch (workClass) {
                case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                case QUERY, CHECKPOINT -> false;
            };
            policies.put(workClass, new WorkClassPolicy(1, maxQueueRecords, 1_000_000,
                    maxQueueRecords, 1_000_000, 1_000, protectedClass ? 1 : 0,
                    protectedClass ? 1 : 0, workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies, 100, 100,
                16, 2_000_000), () -> 0);
    }

    private static final class AtomicAppender implements ShardLogMutationAppender {
        private final Fixture fixture;
        private final java.util.concurrent.atomic.AtomicReference<SystemMutation> last =
                new java.util.concurrent.atomic.AtomicReference<>();
        private ShardLogMutationAppender.AppendOutcome next;
        private int calls;

        private AtomicAppender(final Fixture fixture) {
            this.fixture = fixture;
            next = ShardLogMutationAppender.AppendOutcome.persisted(fixture.position(1));
        }

        @Override
        public ShardLogMutationAppender.AppendOutcome append(final SystemMutation mutation) {
            calls++;
            last.set(mutation);
            if (next.sourcePosition() != null && !next.sourcePosition().shardId().equals(mutation.shardId())) {
                return ShardLogMutationAppender.AppendOutcome.persisted(fixture.position(1));
            }
            return next;
        }
    }

    private record Fixture(ShardId shard, UUID topic, OwnerLease lease, OxiaOwnerLeaseStore authority,
                           OwnedDelayShard owned, WorkClassExecutionRegistry workClasses,
                           SharedRocksDbResources resources, ShardStore store) implements AutoCloseable {
        private KafkaSourcePosition position(final long offset) {
            return new KafkaSourcePosition(shard, "gc-cluster", topic, offset, null, 1_000 + offset);
        }

        @Override
        public void close() {
            store.close();
            resources.close();
        }
    }
}
