package io.nereusstream.delay.ownership;

import io.nereusstream.delay.adapter.BoundedDestinationPublishAdapter;
import io.nereusstream.delay.adapter.DestinationPhysicalAdmission;
import io.nereusstream.delay.adapter.DestinationPublishAdapter;
import io.nereusstream.delay.adapter.DestinationPublishRequest;
import io.nereusstream.delay.adapter.DestinationPublishResult;
import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.SystemMutation;
import io.nereusstream.delay.protocol.SystemMutationType;
import io.nereusstream.delay.runtime.AttemptLedgerState;
import io.nereusstream.delay.runtime.PublishAttemptLedger;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassPolicy;
import io.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.security.KeyPairGenerator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerPhysicalPublishExecutorTest {
    @Test
    void allowedResultIsHandedOffAfterTheBoundedPhysicalCall() {
        final Fixture fixture = new Fixture();
        final AtomicInteger delegateCalls = new AtomicInteger();
        final AtomicReference<SystemMutation> outcome = new AtomicReference<>();
        final DestinationPublishAdapter delegate = request -> {
            delegateCalls.incrementAndGet();
            return CompletableFuture.completedFuture(DestinationPublishResult.published(
                    Bytes.utf8("delivery"), 1_001, Bytes.utf8("ack")));
        };
        final WorkerPhysicalPublishExecutor executor = fixture.executor(delegate, outcome,
                ignored -> WorkerPhysicalPublishExecutor.Decision.allowed());
        try (executor) {
            final WorkerPhysicalPublishExecutor.Submission submission = executor.submit(
                    fixture.attempt, fixture.request, () -> 1_000);

            assertEquals(WorkerPhysicalPublishExecutor.SubmissionState.OUTCOME_HANDOFF_QUEUED,
                    submission.state());
            assertEquals(1, delegateCalls.get());
            assertEquals(StableCode.OK, submission.physicalResult().orElseThrow().stableCode());
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
            return CompletableFuture.completedFuture(DestinationPublishResult.published(
                    Bytes.utf8("delivery"), 1_001, Bytes.utf8("ack")));
        };
        final WorkerPhysicalPublishExecutor executor = fixture.executor(delegate,
                ignored -> outcomeCalls.incrementAndGet(),
                ignored -> WorkerPhysicalPublishExecutor.Decision.deferred(
                        StableCode.CAPABILITY_UNAVAILABLE, Bytes.utf8("not-ready")));
        try (executor) {
            final WorkerPhysicalPublishExecutor.Submission submission = executor.submit(
                    fixture.attempt, fixture.request, () -> 1_000);

            assertEquals(WorkerPhysicalPublishExecutor.SubmissionState.DEFERRED, submission.state());
            assertEquals(0, delegateCalls.get());
            assertEquals(0, outcomeCalls.get());
            assertEquals(StableCode.CAPABILITY_UNAVAILABLE, submission.physicalResult().orElseThrow().stableCode());
            assertEquals(0, fixture.admission.workerSnapshot().activeRequests());
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
            return CompletableFuture.completedFuture(DestinationPublishResult.published(
                    Bytes.utf8("delivery"), 1_001, Bytes.utf8("ack")));
        };
        final WorkerPhysicalPublishExecutor executor = fixture.executor(delegate,
                result -> handed.set(result), ignored -> gateCalls.incrementAndGet() == 1
                        ? WorkerPhysicalPublishExecutor.Decision.allowed()
                        : WorkerPhysicalPublishExecutor.Decision.definitivelyNotPublished(
                                StableCode.CREDENTIAL_BINDING_DRIFT, Bytes.utf8("binding-drift")));
        try (executor) {
            final WorkerPhysicalPublishExecutor.Submission submission = executor.submit(
                    fixture.attempt, fixture.request, () -> 1_000);

            assertEquals(2, gateCalls.get());
            assertEquals(0, delegateCalls.get());
            assertEquals(StableCode.CREDENTIAL_BINDING_DRIFT,
                    submission.physicalResult().orElseThrow().stableCode());
            assertEquals(DestinationPublishResult.Disposition.DEFINITIVELY_NOT_PUBLISHED,
                    handed.get().disposition());
        }
    }

    @Test
    void requestPreparationRejectsAnOpaqueOrMismatchedAdmission() {
        final Fixture fixture = new Fixture();
        assertThrows(IllegalArgumentException.class,
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
                    final io.nereusstream.delay.protocol.SourcePosition exactSourcePosition,
                    final byte[] exactPreparedHash) {
                sourcePosition.set(exactSourcePosition.canonicalBytes());
                preparedHash.set(exactPreparedHash);
                return CompletableFuture.completedFuture(DestinationPublishResult.published(
                        Bytes.utf8("delivery"), 1_001, Bytes.utf8("ack")));
            }
        };
        final WorkerPhysicalPublishExecutor executor = fixture.executor(delegate,
                ignored -> { }, ignored -> WorkerPhysicalPublishExecutor.Decision.allowed());
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
        final AuthorIdentity author = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"),
                7, Bytes.sha256(Bytes.utf8("fence")));
        return SystemMutation.signed(shard, SystemMutationType.PUBLISH_OUTCOME, 9_000, logical, body,
                author.canonicalBytes(), 1,
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
                io.nereusstream.delay.protocol.DelayMessageId.random(shard), 0,
                Bytes.sha256(Bytes.utf8("attempt")), Bytes.sha256(Bytes.utf8("claim")), 7, 1, lane,
                laneIncarnation, Bytes.utf8("owner"), new byte[16], Bytes.sha256(Bytes.utf8("prepared")),
                Bytes.utf8("opaque-admission"),
                new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 0, null, 1_000).canonicalBytes());
        private final DestinationPublishRequest request = new DestinationPublishRequest(lane, laneIncarnation,
                attempt.delayMessageId(), 0, attempt.publishAttemptId(), 1_000, 1_000,
                Bytes.utf8("payload"), Bytes.utf8("metadata"));

        private WorkerPhysicalPublishExecutor executor(final DestinationPublishAdapter delegate,
                                                        final java.util.function.Consumer<DestinationPublishResult>
                                                                sink,
                                                        final java.util.function.Function<PublishAttemptLedger,
                                                                WorkerPhysicalPublishExecutor.Decision> gate) {
            return new WorkerPhysicalPublishExecutor(
                    new BoundedDestinationPublishAdapter(delegate, admission, workClasses(), Runnable::run),
                    (mutation, ownerClock) -> {
                    },
                    (ignoredAttempt, ignoredRequest, ignoredClock) -> gate.apply(ignoredAttempt),
                    (ignoredAttempt, ignoredRequest, result) -> {
                        sink.accept(result);
                        return mutation(shard);
                    },
                    () -> {
                    });
        }

        private WorkerPhysicalPublishExecutor executor(final DestinationPublishAdapter delegate,
                                                        final AtomicReference<SystemMutation> outcome,
                                                        final java.util.function.Function<PublishAttemptLedger,
                                                                WorkerPhysicalPublishExecutor.Decision> gate) {
            return new WorkerPhysicalPublishExecutor(
                    new BoundedDestinationPublishAdapter(delegate, admission, workClasses(), Runnable::run),
                    (mutation, ownerClock) -> outcome.set(mutation),
                    (ignoredAttempt, ignoredRequest, ignoredClock) -> gate.apply(ignoredAttempt),
                    (ignoredAttempt, ignoredRequest, result) -> mutation(shard),
                    () -> {
                    });
        }

        private static DestinationPhysicalAdmission admission(final DestinationLaneId lane,
                                                               final byte[] laneIncarnation) {
            final DestinationPhysicalAdmission result = new DestinationPhysicalAdmission(1, 100);
            result.registerTargetCluster("cluster", 1, 100);
            result.registerLane(new DestinationPhysicalAdmission.LaneSpec(lane, laneIncarnation, "cluster", 0,
                    0, 1, 100, 1, 100));
            result.openReady(lane);
            return result;
        }

        private static WorkClassExecutionRegistry workClasses() {
            final java.util.EnumMap<WorkClass, WorkClassPolicy> policies =
                    new java.util.EnumMap<>(WorkClass.class);
            for (WorkClass workClass : WorkClass.values()) {
                policies.put(workClass, new WorkClassPolicy(1, 1, 1_000_000, 1, 1_000_000, 1_000,
                        1, 1_000_000, workClass == WorkClass.LEASE_FENCE));
            }
            return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies, 100, 100,
                    16, 8_000_000), () -> 0);
        }
    }
}
