package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter;
import com.nereusstream.delay.adapter.DestinationPhysicalAdmission;
import com.nereusstream.delay.adapter.DestinationPublishAdapter;
import com.nereusstream.delay.adapter.DestinationPublishRequest;
import com.nereusstream.delay.adapter.DestinationPublishResult;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.runtime.PublishAttemptLedger;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import java.security.KeyPairGenerator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class WorkerPhysicalPublishExecutorTest {
    @Test
    void completionBookkeepingUsesTheInjectedExecutor() throws Exception {
        final Fixture fixture = new Fixture();
        final CompletableFuture<DestinationPublishResult> physical = new CompletableFuture<>();
        final CountDownLatch handedOff = new CountDownLatch(1);
        final AtomicReference<String> completionThread = new AtomicReference<>();
        final ExecutorService completionExecutor =
                Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "publish-completion-test"));
        final WorkerPhysicalPublishExecutor executor = new WorkerPhysicalPublishExecutor(
                new BoundedDestinationPublishAdapter(
                        ignored -> physical, fixture.admission, Fixture.workClasses(), Runnable::run),
                (mutation, ownerClock) -> {
                    completionThread.set(Thread.currentThread().getName());
                    handedOff.countDown();
                },
                (ignoredAttempt, ignoredRequest, ignoredClock) -> WorkerPhysicalPublishExecutor.Decision.allowed(),
                (ignoredAttempt, ignoredRequest, result) -> mutation(fixture.shard),
                () -> {},
                null,
                completionExecutor);
        try (executor) {
            final WorkerPhysicalPublishExecutor.Submission submission =
                    executor.submit(fixture.attempt, fixture.request, () -> 1_000);
            assertEquals(WorkerPhysicalPublishExecutor.SubmissionState.PENDING, submission.state());

            physical.complete(DestinationPublishResult.published(Bytes.utf8("delivery"), 1_001, Bytes.utf8("ack")));

            assertTrue(handedOff.await(5, TimeUnit.SECONDS));
            assertEquals("publish-completion-test", completionThread.get());
            assertEquals(WorkerPhysicalPublishExecutor.SubmissionState.OUTCOME_HANDOFF_QUEUED, submission.state());
        } finally {
            completionExecutor.shutdownNow();
        }
    }

    @Test
    void rejectedCompletionExecutorFailsClosedWithoutInlineOutcomeHandoff() {
        final Fixture fixture = new Fixture();
        final CompletableFuture<DestinationPublishResult> physical = new CompletableFuture<>();
        final AtomicInteger outcomeCalls = new AtomicInteger();
        final AtomicInteger fenceCalls = new AtomicInteger();
        final WorkerPhysicalPublishExecutor executor = new WorkerPhysicalPublishExecutor(
                new BoundedDestinationPublishAdapter(
                        ignored -> physical, fixture.admission, Fixture.workClasses(), Runnable::run),
                (mutation, ownerClock) -> outcomeCalls.incrementAndGet(),
                (ignoredAttempt, ignoredRequest, ignoredClock) -> WorkerPhysicalPublishExecutor.Decision.allowed(),
                (ignoredAttempt, ignoredRequest, result) -> mutation(fixture.shard),
                fenceCalls::incrementAndGet,
                null,
                ignored -> {
                    throw new java.util.concurrent.RejectedExecutionException("completion queue closed");
                });
        try (executor) {
            final WorkerPhysicalPublishExecutor.Submission submission =
                    executor.submit(fixture.attempt, fixture.request, () -> 1_000);

            physical.complete(DestinationPublishResult.published(Bytes.utf8("delivery"), 1_001, Bytes.utf8("ack")));

            assertEquals(WorkerPhysicalPublishExecutor.SubmissionState.FAILED, submission.state());
            assertEquals(0, outcomeCalls.get());
            assertEquals(1, fenceCalls.get());
            assertTrue(submission.failure().orElseThrow() instanceof java.util.concurrent.RejectedExecutionException);
        }
    }

    @Test
    void allowedResultIsHandedOffAfterTheBoundedPhysicalCall() {
        final Fixture fixture = new Fixture();
        final AtomicInteger delegateCalls = new AtomicInteger();
        final AtomicReference<SystemMutation> outcome = new AtomicReference<>();
        final DestinationPublishAdapter delegate = request -> {
            delegateCalls.incrementAndGet();
            return CompletableFuture.completedFuture(
                    DestinationPublishResult.published(Bytes.utf8("delivery"), 1_001, Bytes.utf8("ack")));
        };
        final WorkerPhysicalPublishExecutor executor =
                fixture.executor(delegate, outcome, ignored -> WorkerPhysicalPublishExecutor.Decision.allowed());
        try (executor) {
            final WorkerPhysicalPublishExecutor.Submission submission =
                    executor.submit(fixture.attempt, fixture.request, () -> 1_000);

            assertEquals(WorkerPhysicalPublishExecutor.SubmissionState.OUTCOME_HANDOFF_QUEUED, submission.state());
            assertEquals(1, delegateCalls.get());
            assertEquals(
                    StableCode.OK, submission.physicalResult().orElseThrow().stableCode());
            assertEquals(outcome.get(), submission.outcomeMutation().orElseThrow());
            assertEquals(0, fixture.admission.workerSnapshot().activeRequests());
            assertTrue(submission.failure().isEmpty());
        }
    }

    @Test
    void deferredGateDoesNotCallTheDestinationOrCreateAnOutcome() {
        final Fixture fixture = new Fixture();
        final AtomicInteger delegateCalls = new AtomicInteger();
        final AtomicInteger outcomeCalls = new AtomicInteger();
        final DestinationPublishAdapter delegate = request -> {
            delegateCalls.incrementAndGet();
            return CompletableFuture.completedFuture(
                    DestinationPublishResult.published(Bytes.utf8("delivery"), 1_001, Bytes.utf8("ack")));
        };
        final WorkerPhysicalPublishExecutor executor = fixture.executor(
                delegate,
                ignored -> outcomeCalls.incrementAndGet(),
                ignored -> WorkerPhysicalPublishExecutor.Decision.deferred(
                        StableCode.CAPABILITY_UNAVAILABLE, Bytes.utf8("not-ready")));
        try (executor) {
            final WorkerPhysicalPublishExecutor.Submission submission =
                    executor.submit(fixture.attempt, fixture.request, () -> 1_000);

            assertEquals(WorkerPhysicalPublishExecutor.SubmissionState.DEFERRED, submission.state());
            assertEquals(0, delegateCalls.get());
            assertEquals(0, outcomeCalls.get());
            assertEquals(
                    StableCode.CAPABILITY_UNAVAILABLE,
                    submission.physicalResult().orElseThrow().stableCode());
            assertEquals(0, fixture.admission.workerSnapshot().activeRequests());
        }
    }

    @Test
    void earlyManagedRequestIsDefinitivelyRejectedBeforePhysicalAdmission() {
        final Fixture fixture = new Fixture();
        final AtomicInteger gateCalls = new AtomicInteger();
        final AtomicInteger delegateCalls = new AtomicInteger();
        final AtomicReference<SystemMutation> outcome = new AtomicReference<>();
        final DestinationPublishAdapter delegate = request -> {
            delegateCalls.incrementAndGet();
            return CompletableFuture.failedFuture(new AssertionError("early request reached the adapter"));
        };
        final WorkerPhysicalPublishExecutor executor = fixture.executor(delegate, outcome, ignored -> {
            gateCalls.incrementAndGet();
            throw new AssertionError("early request reached the live physical gate");
        });
        try (executor) {
            final WorkerPhysicalPublishExecutor.Submission submission =
                    executor.submit(fixture.attempt, fixture.request(900, 1_000), () -> 1_000);

            assertEquals(WorkerPhysicalPublishExecutor.SubmissionState.OUTCOME_HANDOFF_QUEUED, submission.state());
            assertEquals(
                    DestinationPublishResult.Disposition.DEFINITIVELY_NOT_PUBLISHED,
                    submission.physicalResult().orElseThrow().disposition());
            assertEquals(
                    StableCode.CAPABILITY_UNAVAILABLE,
                    submission.physicalResult().orElseThrow().stableCode());
            assertEquals(0, gateCalls.get());
            assertEquals(0, delegateCalls.get());
            assertEquals(0, fixture.admission.workerSnapshot().activeRequests());
            assertTrue(submission.physicalCall().isEmpty());
            assertEquals(outcome.get(), submission.outcomeMutation().orElseThrow());
        }
    }

    @Test
    void lateGateRecheckPreventsTheDelegateCallAndHandsOffDefinitiveResult() {
        final Fixture fixture = new Fixture();
        final AtomicInteger gateCalls = new AtomicInteger();
        final AtomicInteger delegateCalls = new AtomicInteger();
        final AtomicReference<DestinationPublishResult> handed = new AtomicReference<>();
        final DestinationPublishAdapter delegate = request -> {
            delegateCalls.incrementAndGet();
            return CompletableFuture.completedFuture(
                    DestinationPublishResult.published(Bytes.utf8("delivery"), 1_001, Bytes.utf8("ack")));
        };
        final WorkerPhysicalPublishExecutor executor = fixture.executor(
                delegate,
                result -> handed.set(result),
                ignored -> gateCalls.incrementAndGet() == 1
                        ? WorkerPhysicalPublishExecutor.Decision.allowed()
                        : WorkerPhysicalPublishExecutor.Decision.definitivelyNotPublished(
                                StableCode.CREDENTIAL_BINDING_DRIFT, Bytes.utf8("binding-drift")));
        try (executor) {
            final WorkerPhysicalPublishExecutor.Submission submission =
                    executor.submit(fixture.attempt, fixture.request, () -> 1_000);

            assertEquals(2, gateCalls.get());
            assertEquals(0, delegateCalls.get());
            assertEquals(
                    StableCode.CREDENTIAL_BINDING_DRIFT,
                    submission.physicalResult().orElseThrow().stableCode());
            assertEquals(
                    DestinationPublishResult.Disposition.DEFINITIVELY_NOT_PUBLISHED,
                    handed.get().disposition());
        }
    }

    @Test
    void requestPreparationRejectsAnOpaqueOrMismatchedAdmission() {
        final Fixture fixture = new Fixture();
        assertThrows(
                IllegalArgumentException.class,
                () -> WorkerPhysicalPublishExecutor.prepareRequest(fixture.attempt, fixture.request.payload()));
    }

    @Test
    void forwardsLedgerSourcePositionAndPreparedHashThroughBoundedAdapter() {
        final Fixture fixture = new Fixture();
        final AtomicReference<byte[]> sourcePosition = new AtomicReference<>();
        final AtomicReference<byte[]> preparedHash = new AtomicReference<>();
        final DestinationPublishAdapter delegate = new DestinationPublishAdapter() {
            @Override
            public java.util.concurrent.CompletionStage<DestinationPublishResult> publish(
                    final DestinationPublishRequest request) {
                throw new AssertionError("source-bound adapter path was not used");
            }

            @Override
            public java.util.concurrent.CompletionStage<DestinationPublishResult> publish(
                    final DestinationPublishRequest request,
                    final com.nereusstream.delay.protocol.SourcePosition exactSourcePosition,
                    final byte[] exactPreparedHash) {
                sourcePosition.set(exactSourcePosition.canonicalBytes());
                preparedHash.set(exactPreparedHash);
                return CompletableFuture.completedFuture(
                        DestinationPublishResult.published(Bytes.utf8("delivery"), 1_001, Bytes.utf8("ack")));
            }
        };
        final WorkerPhysicalPublishExecutor executor =
                fixture.executor(delegate, ignored -> {}, ignored -> WorkerPhysicalPublishExecutor.Decision.allowed());
        try (executor) {
            executor.submit(fixture.attempt, fixture.request, () -> 1_000);
        }

        assertArrayEquals(fixture.attempt.sourcePosition(), sourcePosition.get());
        assertArrayEquals(fixture.attempt.preparedPublishHash(), preparedHash.get());
    }

    private static SystemMutation mutation(final ShardId shard) {
        final byte[] logical = Bytes.sha256(Bytes.utf8("physical-outcome"));
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        final byte[] nested = CanonicalProtobuf.message(output -> CanonicalProtobuf.uint32(output, 1, 1));
        final byte[] body = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.PUBLISH_OUTCOME.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, logical);
            CanonicalProtobuf.uint32(output, 11, 3);
            CanonicalProtobuf.uint32(output, 12, 4);
            CanonicalProtobuf.uint32(output, 13, StableCode.DESTINATION_OUTCOME_UNKNOWN.wireValue());
            CanonicalProtobuf.bytes(output, 15, nested);
            CanonicalProtobuf.bytes(output, 16, nested);
            CanonicalProtobuf.bytes(output, 17, nested);
        });
        final AuthorIdentity author = AuthorIdentity.owner(
                Bytes.utf8("deployment"), Bytes.utf8("worker"), 7, Bytes.sha256(Bytes.utf8("fence")));
        return SystemMutation.signed(
                shard,
                SystemMutationType.PUBLISH_OUTCOME,
                9_000,
                logical,
                body,
                author.canonicalBytes(),
                1,
                keyPair().getPrivate());
    }

    private static java.security.KeyPair keyPair() {
        try {
            return KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        } catch (java.security.GeneralSecurityException failure) {
            throw new IllegalStateException("Ed25519 is unavailable", failure);
        }
    }

    private static final class Fixture {
        private final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        private final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("physical-lane"));
        private final byte[] laneIncarnation = new byte[16];
        private final DestinationPhysicalAdmission admission = admission(lane, laneIncarnation);
        private final PublishAttemptLedger attempt = PublishAttemptLedger.publishing(
                com.nereusstream.delay.protocol.DelayMessageId.random(shard),
                0,
                Bytes.sha256(Bytes.utf8("attempt")),
                Bytes.sha256(Bytes.utf8("claim")),
                7,
                1,
                lane,
                laneIncarnation,
                Bytes.utf8("owner"),
                new byte[16],
                Bytes.sha256(Bytes.utf8("prepared")),
                Bytes.utf8("opaque-admission"),
                new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 0, null, 1_000).canonicalBytes());
        private final DestinationPublishRequest request = new DestinationPublishRequest(
                lane,
                laneIncarnation,
                attempt.delayMessageId(),
                0,
                attempt.publishAttemptId(),
                1_000,
                1_000,
                Bytes.utf8("payload"),
                Bytes.utf8("metadata"));

        private DestinationPublishRequest request(final long actionAt, final long deliverAt) {
            return new DestinationPublishRequest(
                    lane,
                    laneIncarnation,
                    attempt.delayMessageId(),
                    0,
                    attempt.publishAttemptId(),
                    actionAt,
                    deliverAt,
                    Bytes.utf8("payload"),
                    Bytes.utf8("metadata"));
        }

        private WorkerPhysicalPublishExecutor executor(
                final DestinationPublishAdapter delegate,
                final java.util.function.Consumer<DestinationPublishResult> sink,
                final java.util.function.Function<PublishAttemptLedger, WorkerPhysicalPublishExecutor.Decision> gate) {
            return new WorkerPhysicalPublishExecutor(
                    new BoundedDestinationPublishAdapter(delegate, admission, workClasses(), Runnable::run),
                    (mutation, ownerClock) -> {},
                    (ignoredAttempt, ignoredRequest, ignoredClock) -> gate.apply(ignoredAttempt),
                    (ignoredAttempt, ignoredRequest, result) -> {
                        sink.accept(result);
                        return mutation(shard);
                    },
                    () -> {});
        }

        private WorkerPhysicalPublishExecutor executor(
                final DestinationPublishAdapter delegate,
                final AtomicReference<SystemMutation> outcome,
                final java.util.function.Function<PublishAttemptLedger, WorkerPhysicalPublishExecutor.Decision> gate) {
            return new WorkerPhysicalPublishExecutor(
                    new BoundedDestinationPublishAdapter(delegate, admission, workClasses(), Runnable::run),
                    (mutation, ownerClock) -> outcome.set(mutation),
                    (ignoredAttempt, ignoredRequest, ignoredClock) -> gate.apply(ignoredAttempt),
                    (ignoredAttempt, ignoredRequest, result) -> mutation(shard),
                    () -> {});
        }

        private static DestinationPhysicalAdmission admission(
                final DestinationLaneId lane, final byte[] laneIncarnation) {
            final DestinationPhysicalAdmission result = new DestinationPhysicalAdmission(1, 100);
            result.registerTargetCluster("cluster", 1, 100);
            result.registerLane(
                    new DestinationPhysicalAdmission.LaneSpec(lane, laneIncarnation, "cluster", 0, 0, 1, 100, 1, 100));
            result.openReady(lane);
            return result;
        }

        private static WorkClassExecutionRegistry workClasses() {
            final java.util.EnumMap<WorkClass, WorkClassPolicy> policies = new java.util.EnumMap<>(WorkClass.class);
            for (WorkClass workClass : WorkClass.values()) {
                policies.put(
                        workClass,
                        new WorkClassPolicy(
                                1,
                                1,
                                1_000_000,
                                1,
                                1_000_000,
                                1_000,
                                1,
                                1_000_000,
                                workClass == WorkClass.LEASE_FENCE));
            }
            return new WorkClassExecutionRegistry(
                    new WorkClassRuntimeConfig(policies, 100, 100, 16, 8_000_000), () -> 0);
        }
    }
}
