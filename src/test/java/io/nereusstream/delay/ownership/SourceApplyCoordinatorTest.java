package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntent;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
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
import java.lang.reflect.Modifier;
import java.security.PublicKey;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SourceApplyCoordinatorTest {
    @TempDir
    Path tempDir;

    @Test
    void publicConstructorBuildsTheActiveExecutorFromItsExactDependencies() {
        final var publicConstructors = Arrays.stream(SourceApplyCoordinator.class.getDeclaredConstructors())
                .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
                .toList();

        assertEquals(2, publicConstructors.size());
        assertTrue(publicConstructors.stream()
                .noneMatch(constructor -> Arrays.asList(constructor.getParameterTypes())
                        .contains(SourceApplyWorkClassExecutor.class)));
    }

    @Test
    void advancesSourceCursorOnlyAfterWriteBatchAndBrokerAck() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("acked"))) {
            final AtomicInteger acknowledgements = new AtomicInteger();
            final SourceReplayRecord entry = fixture.entry("ack");
            final SourceApplyCoordinator coordinator = fixture.coordinator(entry, (ignored, outcome) -> {
                assertNotNull(outcome.commandResult());
                assertNotNull(fixture.owned.shard().getMessage(entry.command().delayMessageId()));
                acknowledgements.incrementAndGet();
                return SourceAcknowledgement.AcknowledgementResult.acked();
            });

            final SourceApplyCoordinator.TurnResult result = coordinator.runTurn(fixture.budget(), () -> 101);

            assertEquals(SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED, result.status());
            assertEquals(1, acknowledgements.get());
            assertTrue(coordinator.pendingEntry().isEmpty());
            assertFalse(fixture.source.hasNext());
        }
    }

    @Test
    void unknownAckRetainsExactEntryAndDoesNotReapplyTheWriteBatch() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("unknown"))) {
            final AtomicInteger acknowledgements = new AtomicInteger();
            final SourceReplayRecord entry = fixture.entry("unknown");
            final SourceApplyCoordinator coordinator = fixture.coordinator(entry, (ignored, outcome) -> {
                assertNotNull(outcome.commandResult());
                return acknowledgements.incrementAndGet() == 1
                        ? SourceAcknowledgement.AcknowledgementResult.unknown(null)
                        : SourceAcknowledgement.AcknowledgementResult.acked();
            });

            final SourceApplyCoordinator.TurnResult first = coordinator.runTurn(fixture.budget(), () -> 101);
            assertEquals(SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN, first.status());
            assertEquals(entry, coordinator.pendingEntry().orElseThrow());
            assertTrue(fixture.source.hasNext());

            final SourceApplyCoordinator.TurnResult second = coordinator.runTurn(fixture.budget(), () -> 101);
            assertEquals(SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED, second.status());
            assertEquals(2, acknowledgements.get());
            assertTrue(coordinator.pendingEntry().isEmpty());
            assertFalse(fixture.source.hasNext());
        }
    }

    @Test
    void queueRejectionDoesNotConsumeOrApplySourceEntry() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("rejected"))) {
            final SourceReplayRecord entry = fixture.entry("rejected");
            final SourceApplyCoordinator coordinator = fixture.coordinator(entry,
                    (ignored, outcome) -> SourceAcknowledgement.AcknowledgementResult.acked());
            fixture.workClasses.submit(new WorkClassTask(WorkClass.SOURCE_APPLY, "occupied", 1), () -> {
            });

            final SourceApplyCoordinator.TurnResult rejected = coordinator.runTurn(fixture.budget(), () -> 101);
            assertEquals(SourceApplyCoordinator.TurnStatus.SUBMISSION_REJECTED, rejected.status());
            assertEquals(entry, coordinator.pendingEntry().orElseThrow());
            assertTrue(fixture.owned.shard().getMessage(entry.command().delayMessageId()) == null);
            assertTrue(fixture.source.hasNext());

            fixture.workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));
            final SourceApplyCoordinator.TurnResult applied = coordinator.runTurn(fixture.budget(), () -> 101);
            assertEquals(SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED, applied.status());
            assertFalse(fixture.source.hasNext());
        }
    }

    @Test
    void workerSourceLoopRetainsPollAcrossUnknownAckAndPollsAgainAfterAck() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("worker-source"))) {
            final SourceReplayRecord entry = fixture.entry("worker-source");
            final AtomicInteger polls = new AtomicInteger();
            final AtomicInteger acknowledgements = new AtomicInteger();
            final SourceRecordConsumer consumer = () -> {
                if (polls.incrementAndGet() == 1) {
                    return Optional.of(new SourceRecordConsumer.PolledSourceRecord(entry, (candidate, outcome) -> {
                        assertEquals(entry, candidate);
                        assertNotNull(outcome.commandResult());
                        assertNotNull(fixture.owned.shard().getMessage(entry.command().delayMessageId()));
                        return acknowledgements.incrementAndGet() == 1
                                ? SourceAcknowledgement.AcknowledgementResult.unknown(null)
                                : SourceAcknowledgement.AcknowledgementResult.acked();
                    }));
                }
                return Optional.empty();
            };

            try (WorkerSourceApplyLoop loop = new WorkerSourceApplyLoop(consumer, fixture.workClasses,
                    fixture.owned, fixture.authority, fixture.verificationKey)) {
                final SourceApplyCoordinator.TurnResult first = loop.runTurn(fixture.budget(), () -> 101);
                assertEquals(SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN, first.status());
                assertEquals(entry, loop.pendingEntry().orElseThrow());
                assertEquals(1, polls.get());

                final SourceApplyCoordinator.TurnResult second = loop.runTurn(fixture.budget(), () -> 101);
                assertEquals(SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED, second.status());
                assertTrue(loop.pendingEntry().isEmpty());
                assertEquals(1, polls.get(), "ACK retry must not poll a replacement record");
                assertEquals(2, acknowledgements.get());

                final SourceApplyCoordinator.TurnResult idle = loop.runTurn(fixture.budget(), () -> 101);
                assertEquals(SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE, idle.status());
                assertEquals(2, polls.get());
            }
        }
    }

    @Test
    void workerSourceLoopDoesNotTreatAnIdlePollAsPermanentEndOfStream() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("worker-source-idle"))) {
            final SourceReplayRecord entry = fixture.entry("worker-source-idle");
            final AtomicInteger polls = new AtomicInteger();
            final SourceRecordConsumer consumer = () -> {
                if (polls.incrementAndGet() == 1) {
                    return Optional.empty();
                }
                if (polls.get() == 2) {
                    return Optional.of(new SourceRecordConsumer.PolledSourceRecord(entry,
                            (candidate, outcome) -> SourceAcknowledgement.AcknowledgementResult.acked()));
                }
                return Optional.empty();
            };

            try (WorkerSourceApplyLoop loop = new WorkerSourceApplyLoop(consumer, fixture.workClasses,
                    fixture.owned, fixture.authority, fixture.verificationKey)) {
                assertEquals(SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE,
                        loop.runTurn(fixture.budget(), () -> 101).status());
                assertEquals(SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED,
                        loop.runTurn(fixture.budget(), () -> 101).status());
                assertEquals(2, polls.get());
            }
        }
    }

    @Test
    void workerShardRuntimeBlocksDrainUntilPendingAckIsResolvedAndClosesSourceAfterRelease() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("worker-runtime"))) {
            final SourceReplayRecord entry = fixture.entry("worker-runtime");
            final AtomicInteger polls = new AtomicInteger();
            final AtomicInteger acknowledgements = new AtomicInteger();
            final AtomicInteger closeCalls = new AtomicInteger();
            final AtomicInteger stopCalls = new AtomicInteger();
            final SourceRecordConsumer consumer = new SourceRecordConsumer() {
                @Override
                public Optional<PolledSourceRecord> poll() {
                    if (polls.incrementAndGet() != 1) {
                        return Optional.empty();
                    }
                    return Optional.of(new PolledSourceRecord(entry, (candidate, outcome) -> {
                        assertEquals(entry, candidate);
                        return acknowledgements.incrementAndGet() == 1
                                ? SourceAcknowledgement.AcknowledgementResult.unknown(null)
                                : SourceAcknowledgement.AcknowledgementResult.acked();
                    }));
                }

                @Override
                public void close() {
                    closeCalls.incrementAndGet();
                }
            };

            final WorkerShardRuntime runtime = new WorkerShardRuntime(consumer, fixture.workClasses,
                    fixture.owned, fixture.store, fixture.resources, fixture.authority, fixture.verificationKey);
            assertEquals(SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN,
                    runtime.runSourceTurn(fixture.budget(), () -> 101).status());
            assertThrows(IllegalStateException.class, () -> runtime.drain(
                    new OwnerDrainCoordinator.DrainRequest(5_000, 0, null), () -> 101,
                    stopCalls::incrementAndGet));
            assertFalse(runtime.sourcePaused());
            assertEquals(SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED,
                    runtime.runSourceTurn(fixture.budget(), () -> 101).status());

            final OwnerDrainCoordinator.DrainResult result = runtime.drain(
                    new OwnerDrainCoordinator.DrainRequest(5_000, 0, null), () -> 101,
                    stopCalls::incrementAndGet);

            assertEquals(0, result.revokedClaims());
            assertTrue(runtime.sourcePaused());
            assertTrue(runtime.pendingSourceEntry().isEmpty());
            assertEquals(1, stopCalls.get());
            assertEquals(1, closeCalls.get());
            assertTrue(fixture.store.isClosed());
            assertTrue(fixture.backend.current(fixture.shard).isEmpty());
            assertThrows(IllegalStateException.class, () -> runtime.runSourceTurn(
                    fixture.budget(), () -> 101));
            runtime.close();
        }
    }

    private static WorkClassExecutionRegistry workClasses() {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass = switch (workClass) {
                case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                case QUERY, CHECKPOINT -> false;
            };
            policies.put(workClass, new WorkClassPolicy(1, 1, 1_000_000,
                    1, 1_000_000, 1_000, protectedClass ? 1 : 0, protectedClass ? 1 : 0,
                    workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies, 100, 100,
                16, 2_000_000), () -> 0);
    }

    private static final class Fixture implements AutoCloseable {
        private final ShardId shard;
        private final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        private final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        private SourceReplayCursor<SourceReplayEntry> source;
        private final UUID topic;
        private final WorkClassExecutionRegistry workClasses = workClasses();
        private final OwnedDelayShard owned;
        private final PublicKey verificationKey;
        private final SharedRocksDbResources resources;
        private final ShardStore store;

        private Fixture(final Path root) throws Exception {
            shard = new ShardId(RouteIncarnation.random(), 4);
            topic = UUID.randomUUID();
            final SourceAssignment assignment = new SourceAssignment(shard,
                    Bytes.sha256(Bytes.utf8("coordinator-assignment")), 1,
                    new KafkaActivationBarrier(shard, "coordinator-cluster", topic, 0));
            final OwnerLease lease = backend.acquire(assignment, "coordinator-owner",
                    Bytes.sha256(Bytes.utf8("coordinator-session")), 100, 100).orElseThrow();
            final ShardStoreConfig config = ShardStoreConfig.defaults(root);
            resources = new SharedRocksDbResources(config);
            store = ShardStore.open(config, shard, resources);
            owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            owned.markCatchingUp(authority, assignment, SourceReplaySuccessor.strictKafka(), 101);
            owned.activateForCommands(authority, 101);
            verificationKey = KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPublic();
            source = SourceReplayCursor.of(List.<SourceReplayEntry>of().iterator());
        }

        private SourceReplayRecord entry(final String suffix) {
            final PreparedCommand command = PreparedCommand.schedule(shard,
                    new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("coordinator-lane-" + suffix)),
                            2_000, 5_000, OrderingMode.BEST_EFFORT, Bytes.utf8(suffix)), 10_000);
            final KafkaSourcePosition position = new KafkaSourcePosition(shard, "coordinator-cluster", topic,
                    0, null, 1_000);
            final SourceReplayRecord result = new SourceReplayRecord(command, position, null, null);
            // Replace the empty fixture cursor only before the first turn.
            source = SourceReplayCursor.of(List.<SourceReplayEntry>of(result).iterator());
            return result;
        }

        private SourceApplyCoordinator coordinator(final SourceReplayRecord entry,
                                                   final SourceAcknowledgement acknowledgement) {
            return new SourceApplyCoordinator(source, workClasses, owned, authority,
                    verificationKey, acknowledgement);
        }

        private SchedulerBudget budget() {
            return new SchedulerBudget(1, 1_000_000, 1_000);
        }

        @Override
        public void close() {
            store.close();
            resources.close();
        }
    }
}
