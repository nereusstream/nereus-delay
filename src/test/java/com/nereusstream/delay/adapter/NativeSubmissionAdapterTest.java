package com.nereusstream.delay.adapter;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.KafkaMetadata;
import com.nereusstream.delay.protocol.NativeCapabilitySnapshot;
import com.nereusstream.delay.protocol.NativePreparedDelivery;
import com.nereusstream.delay.protocol.NonPersistenceProofKind;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarMetadata;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class NativeSubmissionAdapterTest {
    @Test
    void validNativeRequestIsDefinitivelyRejectedBeforeTransport() throws Exception {
        final Fixture fixture = fixture(4_000, 3_000);
        final AtomicBoolean called = new AtomicBoolean();
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport = request -> {
            called.set(true);
            throw new AssertionError("valid native request reached transport");
        };
        try (PinnedPulsarNativeSubmissionAdapter adapter = fixture.adapter(transport)) {
            final SubmissionOutcomeMessage outcome = adapter.submit(fixture.prepared, attempt(1))
                    .toCompletableFuture()
                    .join();
            assertEquals(
                    com.nereusstream.delay.protocol.SubmissionOutcomeKind.NATIVE_DEFINITELY_NOT_QUEUED, outcome.kind());
            assertEquals(
                    StableCode.CAPABILITY_UNAVAILABLE,
                    outcome.nativeDefinitelyNotQueued().error().code());
            assertEquals(
                    NonPersistenceProofKind.LOCAL_BEFORE_PRODUCER_OWNERSHIP,
                    outcome.nativeDefinitelyNotQueued().proof().kind());
            assertFalse(called.get());
            assertEquals(outcome, SubmissionOutcomeMessage.decode(outcome.canonicalBytes()));
        }
    }

    @Test
    void validNativeRequestDoesNotUseBrokerGuardRejectionProof() throws Exception {
        final Fixture fixture = fixture(4_000, 3_000);
        final AtomicBoolean called = new AtomicBoolean();
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport = request -> {
            called.set(true);
            return CompletableFuture.completedFuture(PulsarSendResult.definitelyNotPersisted(
                    StableCode.BROKER_DEFINITIVE_NOT_PERSISTED.wireValue(), Bytes.utf8("unexpected")));
        };
        try (PinnedPulsarNativeSubmissionAdapter adapter = fixture.adapter(transport)) {
            final SubmissionOutcomeMessage outcome = adapter.submit(fixture.prepared, attempt(2))
                    .toCompletableFuture()
                    .join();
            assertEquals(
                    com.nereusstream.delay.protocol.SubmissionOutcomeKind.NATIVE_DEFINITELY_NOT_QUEUED, outcome.kind());
            assertEquals(
                    NonPersistenceProofKind.LOCAL_BEFORE_PRODUCER_OWNERSHIP,
                    outcome.nativeDefinitelyNotQueued().proof().kind());
            assertEquals(
                    StableCode.CAPABILITY_UNAVAILABLE,
                    outcome.nativeDefinitelyNotQueued().error().code());
            assertFalse(called.get());
            assertEquals(outcome, SubmissionOutcomeMessage.decode(outcome.canonicalBytes()));
        }
    }

    @Test
    void invalidSignatureAndExpiryAreLocalDefiniteBeforeTransport() throws Exception {
        final Fixture fixture = fixture(4_000, 3_000);
        final AtomicBoolean called = new AtomicBoolean();
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport = request -> {
            called.set(true);
            return CompletableFuture.failedFuture(new AssertionError("transport must not be called"));
        };
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        try (PinnedPulsarNativeSubmissionAdapter adapter = new PinnedPulsarNativeSubmissionAdapter(
                fixture.resource, keyPairGenerator.generateKeyPair().getPublic(), fixture.clock, transport)) {
            final SubmissionOutcomeMessage outcome = adapter.submit(fixture.prepared, attempt(4))
                    .toCompletableFuture()
                    .join();
            assertEquals(
                    com.nereusstream.delay.protocol.SubmissionOutcomeKind.NATIVE_DEFINITELY_NOT_QUEUED, outcome.kind());
            assertEquals(
                    StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE,
                    outcome.nativeDefinitelyNotQueued().error().code());
        }

        final Fixture expired = fixture(4_000, 4_000);
        try (PinnedPulsarNativeSubmissionAdapter adapter = expired.adapter(transport)) {
            final SubmissionOutcomeMessage outcome = adapter.submit(expired.prepared, attempt(5))
                    .toCompletableFuture()
                    .join();
            assertEquals(
                    StableCode.NATIVE_PREPARED_SUBMISSION_EXPIRED,
                    outcome.nativeDefinitelyNotQueued().error().code());
        }
        assertFalse(called.get());
    }

    @Test
    void credentialFingerprintDriftIsRejectedBeforeProducerOwnership() throws Exception {
        final Fixture fixture = fixture(4_000, 3_000);
        final AtomicBoolean called = new AtomicBoolean();
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport = request -> {
            called.set(true);
            return CompletableFuture.failedFuture(new AssertionError("transport must not be called"));
        };
        try (PinnedPulsarNativeSubmissionAdapter adapter = new PinnedPulsarNativeSubmissionAdapter(
                fixture.resource,
                fixture.keyPair.getPublic(),
                fixture.clock,
                transport,
                prepared -> Bytes.sha256(Bytes.utf8("rotated-credential")))) {
            final SubmissionOutcomeMessage outcome = adapter.submit(fixture.prepared, attempt(41))
                    .toCompletableFuture()
                    .join();
            assertEquals(
                    StableCode.CREDENTIAL_BINDING_DRIFT,
                    outcome.nativeDefinitelyNotQueued().error().code());
            assertEquals(
                    NonPersistenceProofKind.LOCAL_BEFORE_PRODUCER_OWNERSHIP,
                    outcome.nativeDefinitelyNotQueued().proof().kind());
        }
        assertFalse(called.get());
    }

    @Test
    void unavailableCredentialFingerprintIsTypedBeforeProducerOwnership() throws Exception {
        final Fixture fixture = fixture(4_000, 3_000);
        final AtomicBoolean called = new AtomicBoolean();
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport = request -> {
            called.set(true);
            return CompletableFuture.failedFuture(new AssertionError("transport must not be called"));
        };
        try (PinnedPulsarNativeSubmissionAdapter adapter = new PinnedPulsarNativeSubmissionAdapter(
                fixture.resource, fixture.keyPair.getPublic(), fixture.clock, transport, prepared -> null)) {
            final SubmissionOutcomeMessage outcome = adapter.submit(fixture.prepared, attempt(42))
                    .toCompletableFuture()
                    .join();
            assertEquals(
                    StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE,
                    outcome.nativeDefinitelyNotQueued().error().code());
        }
        assertFalse(called.get());
    }

    @Test
    void unavailableOrInvalidClockIsTypedBeforeProducerOwnership() throws Exception {
        final Fixture fixture = fixture(4_000, 3_000);
        final AtomicBoolean called = new AtomicBoolean();
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport = request -> {
            called.set(true);
            return CompletableFuture.failedFuture(new AssertionError("transport must not be called"));
        };
        final Clock throwingClock = new Clock() {
            @Override
            public java.time.ZoneId getZone() {
                return ZoneOffset.UTC;
            }

            @Override
            public Clock withZone(final java.time.ZoneId zone) {
                return this;
            }

            @Override
            public Instant instant() {
                throw new IllegalStateException("clock unavailable");
            }
        };
        try (PinnedPulsarNativeSubmissionAdapter adapter = new PinnedPulsarNativeSubmissionAdapter(
                fixture.resource, fixture.keyPair.getPublic(), throwingClock, transport)) {
            final SubmissionOutcomeMessage outcome = adapter.submit(fixture.prepared, attempt(43))
                    .toCompletableFuture()
                    .join();
            assertEquals(
                    StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE,
                    outcome.nativeDefinitelyNotQueued().error().code());
        }

        final Clock negativeClock = Clock.fixed(Instant.ofEpochMilli(-1), ZoneOffset.UTC);
        try (PinnedPulsarNativeSubmissionAdapter adapter = new PinnedPulsarNativeSubmissionAdapter(
                fixture.resource, fixture.keyPair.getPublic(), negativeClock, transport)) {
            final SubmissionOutcomeMessage outcome = adapter.submit(fixture.prepared, attempt(44))
                    .toCompletableFuture()
                    .join();
            assertEquals(
                    StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE,
                    outcome.nativeDefinitelyNotQueued().error().code());
        }
        assertFalse(called.get());
    }

    @Test
    void mismatchedPinnedTargetIsRejectedBeforeTransport() throws Exception {
        final Fixture fixture = fixture(4_000, 3_000);
        final AtomicBoolean called = new AtomicBoolean();
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport = request -> {
            called.set(true);
            return CompletableFuture.completedFuture(
                    PulsarSendResult.unknown(StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN.wireValue(), null));
        };
        final PulsarTargetResource mismatched = new PulsarTargetResource(
                fixture.resource.authenticatedClusterId(),
                Bytes.sha256(Bytes.utf8("different-resource")),
                fixture.resource.physicalTopic(),
                fixture.resource.physicalTopicCreationTimestamp(),
                fixture.resource.partition());
        try (PinnedPulsarNativeSubmissionAdapter adapter = new PinnedPulsarNativeSubmissionAdapter(
                mismatched, fixture.keyPair.getPublic(), fixture.clock, transport)) {
            final SubmissionOutcomeMessage outcome = adapter.submit(fixture.prepared, attempt(6))
                    .toCompletableFuture()
                    .join();
            assertEquals(
                    StableCode.PREPARED_SUBMISSION_MISMATCH,
                    outcome.nativeDefinitelyNotQueued().error().code());
        }
        assertFalse(called.get());
    }

    @Test
    void preparedSubmissionAdapterKeepsManagedBranchManaged() throws Exception {
        final Fixture fixture = fixture(4_000, 3_000);
        final PreparedCommand command = managedCommand();
        final AtomicBoolean closed = new AtomicBoolean();
        final WireCommandIngressAdapter managed = new WireCommandIngressAdapter() {
            @Override
            public java.util.concurrent.CompletionStage<com.nereusstream.delay.client.EnqueueOutcome> enqueue(
                    final PreparedCommand ignored) {
                return CompletableFuture.failedFuture(new AssertionError("legacy managed path was used"));
            }

            @Override
            public java.util.concurrent.CompletionStage<com.nereusstream.delay.protocol.EnqueueOutcomeMessage>
                    enqueueOutcome(final PreparedCommand actual, final long queryUntil, final byte[] attemptId) {
                assertEquals(command, actual);
                assertEquals(9_000, queryUntil);
                assertArrayEquals(attempt(7), attemptId);
                return CompletableFuture.completedFuture(
                        WireIngressOutcomeSupport.localDefinite(actual, StableCode.CLIENT_CLOSED));
            }

            @Override
            public void close() {
                closed.set(true);
            }
        };
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport =
                request -> CompletableFuture.failedFuture(new AssertionError("native branch was selected"));
        try (PinnedPulsarNativeSubmissionAdapter nativeAdapter = fixture.adapter(transport);
                PreparedSubmissionAdapter adapter = new PreparedSubmissionAdapter(managed, nativeAdapter)) {
            final SubmissionOutcomeMessage outcome = adapter.submit(
                            com.nereusstream.delay.protocol.PreparedSubmission.managed(
                                    CommandCodec.encodeManagedFrame(command)),
                            9_000,
                            attempt(7))
                    .toCompletableFuture()
                    .join();
            assertEquals(com.nereusstream.delay.protocol.SubmissionOutcomeKind.MANAGED, outcome.kind());
            assertEquals(
                    StableCode.CLIENT_CLOSED,
                    outcome.managed().definitelyNotQueued().error().code());
        }
        assertTrue(closed.get());
    }

    @Test
    void preparedSubmissionAdapterBindsManagedBranchToRoutePolicy() throws Exception {
        final Fixture fixture = fixture(4_000, 3_000);
        final PreparedCommand command = managedCommand();
        final QueuedReceiptQueryPolicy policy = new QueuedReceiptQueryPolicy(11, 4_000);
        final AtomicBoolean strictInvoked = new AtomicBoolean();
        final PolicyBoundWireCommandIngressAdapter managed = new PolicyBoundWireCommandIngressAdapter() {
            @Override
            public java.util.concurrent.CompletionStage<com.nereusstream.delay.client.EnqueueOutcome> enqueue(
                    final PreparedCommand ignored) {
                return CompletableFuture.failedFuture(new AssertionError("legacy managed path was used"));
            }

            @Override
            public java.util.concurrent.CompletionStage<com.nereusstream.delay.protocol.EnqueueOutcomeMessage>
                    enqueueOutcome(final PreparedCommand actual, final long queryUntil, final byte[] attemptId) {
                return CompletableFuture.failedFuture(new AssertionError("absolute boundary path was used"));
            }

            @Override
            public java.util.concurrent.CompletionStage<com.nereusstream.delay.protocol.EnqueueOutcomeMessage>
                    enqueueOutcome(
                            final PreparedCommand actual,
                            final QueuedReceiptQueryPolicy actualPolicy,
                            final byte[] attemptId) {
                strictInvoked.set(true);
                assertEquals(command, actual);
                assertEquals(policy, actualPolicy);
                assertArrayEquals(attempt(12), attemptId);
                return CompletableFuture.completedFuture(
                        WireIngressOutcomeSupport.localDefinite(actual, StableCode.CLIENT_CLOSED));
            }

            @Override
            public void close() {
                // no-op
            }
        };
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport =
                request -> CompletableFuture.failedFuture(new AssertionError("native branch was selected"));
        try (PinnedPulsarNativeSubmissionAdapter nativeAdapter = fixture.adapter(transport);
                PreparedSubmissionAdapter adapter = new PreparedSubmissionAdapter(managed, nativeAdapter)) {
            final SubmissionOutcomeMessage outcome = adapter.submit(
                            com.nereusstream.delay.protocol.PreparedSubmission.managed(
                                    CommandCodec.encodeManagedFrame(command)),
                            policy,
                            attempt(12))
                    .toCompletableFuture()
                    .join();
            assertEquals(com.nereusstream.delay.protocol.SubmissionOutcomeKind.MANAGED, outcome.kind());
            assertEquals(
                    StableCode.CLIENT_CLOSED,
                    outcome.managed().definitelyNotQueued().error().code());
        }
        assertTrue(strictInvoked.get());
    }

    @Test
    void preparedSubmissionAdapterFencesManagedSubmissionAfterClose() throws Exception {
        final Fixture fixture = fixture(4_000, 3_000);
        final PreparedCommand command = managedCommand();
        final AtomicBoolean invoked = new AtomicBoolean();
        final WireCommandIngressAdapter managed = new WireCommandIngressAdapter() {
            @Override
            public java.util.concurrent.CompletionStage<com.nereusstream.delay.client.EnqueueOutcome> enqueue(
                    final PreparedCommand ignored) {
                return CompletableFuture.failedFuture(new AssertionError("legacy managed path was used"));
            }

            @Override
            public java.util.concurrent.CompletionStage<com.nereusstream.delay.protocol.EnqueueOutcomeMessage>
                    enqueueOutcome(final PreparedCommand actual, final long queryUntil, final byte[] attemptId) {
                invoked.set(true);
                return CompletableFuture.completedFuture(
                        WireIngressOutcomeSupport.localDefinite(actual, StableCode.INTEGRITY_ERROR));
            }
        };
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport =
                request -> CompletableFuture.failedFuture(new AssertionError("native branch was selected"));
        try (PinnedPulsarNativeSubmissionAdapter nativeAdapter = fixture.adapter(transport)) {
            final PreparedSubmissionAdapter adapter = new PreparedSubmissionAdapter(managed, nativeAdapter);
            adapter.close();
            final SubmissionOutcomeMessage outcome = adapter.submit(
                            com.nereusstream.delay.protocol.PreparedSubmission.managed(
                                    CommandCodec.encodeManagedFrame(command)),
                            9_000,
                            attempt(9))
                    .toCompletableFuture()
                    .join();
            assertEquals(com.nereusstream.delay.protocol.SubmissionOutcomeKind.MANAGED, outcome.kind());
            assertEquals(
                    StableCode.CLIENT_CLOSED,
                    outcome.managed().definitelyNotQueued().error().code());
            assertFalse(invoked.get());
        }
    }

    @Test
    void preparedSubmissionWrapperRegistrationFailureRemainsManagedUncertain() throws Exception {
        final Fixture fixture = fixture(4_000, 3_000);
        final PreparedCommand command = managedCommand();
        final AtomicBoolean closed = new AtomicBoolean();
        final WireCommandIngressAdapter managed = new WireCommandIngressAdapter() {
            @Override
            public java.util.concurrent.CompletionStage<com.nereusstream.delay.client.EnqueueOutcome> enqueue(
                    final PreparedCommand ignored) {
                return CompletableFuture.failedFuture(new AssertionError("legacy managed path was used"));
            }

            @Override
            public java.util.concurrent.CompletionStage<com.nereusstream.delay.protocol.EnqueueOutcomeMessage>
                    enqueueOutcome(final PreparedCommand actual, final long queryUntil, final byte[] attemptId) {
                assertEquals(command, actual);
                assertEquals(9_000, queryUntil);
                assertArrayEquals(attempt(8), attemptId);
                return new HandleRegistrationFailureFuture<>();
            }

            @Override
            public void close() {
                closed.set(true);
            }
        };
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport =
                request -> CompletableFuture.failedFuture(new AssertionError("native branch was selected"));
        try (PinnedPulsarNativeSubmissionAdapter nativeAdapter = fixture.adapter(transport);
                PreparedSubmissionAdapter adapter = new PreparedSubmissionAdapter(managed, nativeAdapter)) {
            final SubmissionOutcomeMessage outcome = adapter.submit(
                            com.nereusstream.delay.protocol.PreparedSubmission.managed(
                                    CommandCodec.encodeManagedFrame(command)),
                            9_000,
                            attempt(8))
                    .toCompletableFuture()
                    .join();
            assertEquals(com.nereusstream.delay.protocol.SubmissionOutcomeKind.MANAGED, outcome.kind());
            assertEquals(
                    StableCode.ENQUEUE_RESULT_UNCERTAIN,
                    outcome.managed().uncertain().error().code());
            assertArrayEquals(attempt(8), outcome.managed().uncertain().physicalEnqueueAttemptId());
        }
        assertTrue(closed.get());
    }

    @Test
    void preparedSubmissionWrapperNullHandledStageRemainsManagedUncertain() throws Exception {
        final Fixture fixture = fixture(4_000, 3_000);
        final PreparedCommand command = managedCommand();
        final WireCommandIngressAdapter managed = new WireCommandIngressAdapter() {
            @Override
            public java.util.concurrent.CompletionStage<com.nereusstream.delay.client.EnqueueOutcome> enqueue(
                    final PreparedCommand ignored) {
                return CompletableFuture.failedFuture(new AssertionError("legacy managed path was used"));
            }

            @Override
            public java.util.concurrent.CompletionStage<com.nereusstream.delay.protocol.EnqueueOutcomeMessage>
                    enqueueOutcome(final PreparedCommand actual, final long queryUntil, final byte[] attemptId) {
                assertEquals(command, actual);
                assertEquals(9_000, queryUntil);
                assertArrayEquals(attempt(9), attemptId);
                return new NullHandledFuture<>();
            }
        };
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport =
                request -> CompletableFuture.failedFuture(new AssertionError("native branch was selected"));
        try (PinnedPulsarNativeSubmissionAdapter nativeAdapter = fixture.adapter(transport);
                PreparedSubmissionAdapter adapter = new PreparedSubmissionAdapter(managed, nativeAdapter)) {
            final SubmissionOutcomeMessage outcome = adapter.submit(
                            com.nereusstream.delay.protocol.PreparedSubmission.managed(
                                    CommandCodec.encodeManagedFrame(command)),
                            9_000,
                            attempt(9))
                    .toCompletableFuture()
                    .join();
            assertEquals(com.nereusstream.delay.protocol.SubmissionOutcomeKind.MANAGED, outcome.kind());
            assertEquals(
                    StableCode.ENQUEUE_RESULT_UNCERTAIN,
                    outcome.managed().uncertain().error().code());
            assertArrayEquals(attempt(9), outcome.managed().uncertain().physicalEnqueueAttemptId());
        }
    }

    @Test
    void preparedSubmissionWrapperExceptionalStageRemainsManagedUncertain() throws Exception {
        final Fixture fixture = fixture(4_000, 3_000);
        final PreparedCommand command = managedCommand();
        final AtomicBoolean nativeCalled = new AtomicBoolean();
        final WireCommandIngressAdapter managed = new WireCommandIngressAdapter() {
            @Override
            public java.util.concurrent.CompletionStage<com.nereusstream.delay.client.EnqueueOutcome> enqueue(
                    final PreparedCommand ignored) {
                return CompletableFuture.failedFuture(new AssertionError("legacy managed path was used"));
            }

            @Override
            public java.util.concurrent.CompletionStage<com.nereusstream.delay.protocol.EnqueueOutcomeMessage>
                    enqueueOutcome(final PreparedCommand actual, final long queryUntil, final byte[] attemptId) {
                assertEquals(command, actual);
                assertEquals(9_000, queryUntil);
                assertArrayEquals(attempt(10), attemptId);
                return CompletableFuture.failedFuture(new IllegalStateException("managed completion failed"));
            }
        };
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport = request -> {
            nativeCalled.set(true);
            return CompletableFuture.failedFuture(new AssertionError("native branch was selected"));
        };
        try (PinnedPulsarNativeSubmissionAdapter nativeAdapter = fixture.adapter(transport);
                PreparedSubmissionAdapter adapter = new PreparedSubmissionAdapter(managed, nativeAdapter)) {
            final SubmissionOutcomeMessage outcome = adapter.submit(
                            com.nereusstream.delay.protocol.PreparedSubmission.managed(
                                    CommandCodec.encodeManagedFrame(command)),
                            9_000,
                            attempt(10))
                    .toCompletableFuture()
                    .join();
            assertEquals(com.nereusstream.delay.protocol.SubmissionOutcomeKind.MANAGED, outcome.kind());
            assertEquals(
                    StableCode.ENQUEUE_RESULT_UNCERTAIN,
                    outcome.managed().uncertain().error().code());
            assertArrayEquals(attempt(10), outcome.managed().uncertain().physicalEnqueueAttemptId());
        }
        assertFalse(nativeCalled.get());
    }

    @Test
    void preparedSubmissionWrapperInvalidAttemptRemainsLocalDefinite() throws Exception {
        final Fixture fixture = fixture(4_000, 3_000);
        final PreparedCommand command = managedCommand();
        final WireCommandIngressAdapter managed = new WireCommandIngressAdapter() {
            @Override
            public java.util.concurrent.CompletionStage<com.nereusstream.delay.client.EnqueueOutcome> enqueue(
                    final PreparedCommand ignored) {
                return CompletableFuture.failedFuture(new AssertionError("legacy managed path was used"));
            }

            @Override
            public java.util.concurrent.CompletionStage<com.nereusstream.delay.protocol.EnqueueOutcomeMessage>
                    enqueueOutcome(final PreparedCommand actual, final long queryUntil, final byte[] attemptId) {
                assertEquals(command, actual);
                assertEquals(9_000, queryUntil);
                assertEquals(null, attemptId);
                return new HandleRegistrationFailureFuture<>();
            }

            @Override
            public void close() {
                // no-op
            }
        };
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport =
                request -> CompletableFuture.failedFuture(new AssertionError("native branch was selected"));
        try (PinnedPulsarNativeSubmissionAdapter nativeAdapter = fixture.adapter(transport);
                PreparedSubmissionAdapter adapter = new PreparedSubmissionAdapter(managed, nativeAdapter)) {
            final SubmissionOutcomeMessage outcome = adapter.submit(
                            com.nereusstream.delay.protocol.PreparedSubmission.managed(
                                    CommandCodec.encodeManagedFrame(command)),
                            9_000,
                            null)
                    .toCompletableFuture()
                    .join();
            assertEquals(com.nereusstream.delay.protocol.SubmissionOutcomeKind.MANAGED, outcome.kind());
            assertEquals(
                    StableCode.INVALID_PREPARED_COMMAND,
                    outcome.managed().definitelyNotQueued().error().code());
            assertEquals(
                    NonPersistenceProofKind.LOCAL_BEFORE_PRODUCER_OWNERSHIP,
                    outcome.managed().definitelyNotQueued().proof().kind());
        }
    }

    private static PreparedCommand managedCommand() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 11);
        final ProfileRef destination = new ProfileRef(
                Bytes.utf8("managed-destination"),
                1,
                Bytes.sha256(Bytes.utf8("managed-destination-semantic")),
                ProfileKind.DESTINATION);
        final RetryPolicyRef retryPolicy =
                new RetryPolicyRef(Bytes.utf8("managed-retry"), 1, Bytes.sha256(Bytes.utf8("managed-retry-semantic")));
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                destination,
                retryPolicy,
                2_000,
                5_000,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                Bytes.utf8("managed-payload"),
                null,
                AdapterMetadata.kafka(new KafkaMetadata(null, java.util.List.of())),
                null,
                null);
        return PreparedCommand.schedule(shard, intent, 8_000);
    }

    private static Fixture fixture(final long expiry, final long now) throws Exception {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final byte[] resourceBytes = nonZero(32, 7);
        final PulsarBrokerResourceIdentity target = new PulsarBrokerResourceIdentity(
                "native-cluster", resourceBytes, "persistent://tenant/ns/native", Long.MIN_VALUE);
        final ProfileRef destination = new ProfileRef(
                Bytes.utf8("native-destination"),
                1,
                Bytes.sha256(Bytes.utf8("native-destination-semantic")),
                ProfileKind.DESTINATION);
        final ProfileRef capability = new ProfileRef(
                Bytes.utf8("native-capability"),
                1,
                Bytes.sha256(Bytes.utf8("native-capability-semantic")),
                ProfileKind.DELIVERY_CAPABILITY);
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(
                2_100,
                2_110,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("native-clock"),
                1,
                2,
                3,
                Bytes.sha256(Bytes.utf8("native-sample")),
                0,
                null);
        final NativeCapabilitySnapshot snapshot = NativeCapabilitySnapshot.create(
                destination,
                capability,
                target,
                0,
                Bytes.sha256(Bytes.utf8("native-guard")),
                1,
                1,
                Bytes.sha256(Bytes.utf8("native-binding")),
                Bytes.sha256(Bytes.utf8("native-fingerprint")),
                Bytes.sha256(Bytes.utf8("native-scope")),
                issuedAt,
                expiry,
                1,
                keyPair.getPrivate());
        final NativePreparedDelivery prepared = NativePreparedDelivery.create(
                nonZero(32, 8),
                destination,
                capability,
                target,
                0,
                Bytes.utf8("native-payload"),
                new PulsarMetadata(null, null, null, java.util.List.of()),
                null,
                2_200,
                2_300,
                snapshot);
        final PulsarTargetResource resource = new PulsarTargetResource(
                target.authenticatedClusterId(),
                target.resourceIncarnation(),
                target.physicalTopic(),
                target.physicalTopicCreationTimestamp(),
                0);
        return new Fixture(keyPair, resource, prepared, Clock.fixed(Instant.ofEpochMilli(now), ZoneOffset.UTC));
    }

    private static byte[] attempt(final int firstByte) {
        return nonZero(16, firstByte);
    }

    private static byte[] nonZero(final int length, final int firstByte) {
        final byte[] value = new byte[length];
        value[0] = (byte) firstByte;
        return value;
    }

    private static final class HandleRegistrationFailureFuture<T> extends CompletableFuture<T> {
        @Override
        public <U> CompletableFuture<U> thenApply(final java.util.function.Function<? super T, ? extends U> function) {
            throw new IllegalStateException("completion callback registration failed");
        }

        @Override
        public <U> CompletableFuture<U> handle(
                final java.util.function.BiFunction<? super T, Throwable, ? extends U> function) {
            throw new IllegalStateException("completion callback registration failed");
        }
    }

    private static final class NullHandledFuture<T> extends CompletableFuture<T> {
        @Override
        public <U> CompletableFuture<U> handle(
                final java.util.function.BiFunction<? super T, Throwable, ? extends U> function) {
            return null;
        }
    }

    private record Fixture(
            KeyPair keyPair, PulsarTargetResource resource, NativePreparedDelivery prepared, Clock clock) {
        private PinnedPulsarNativeSubmissionAdapter adapter(
                final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport) {
            return new PinnedPulsarNativeSubmissionAdapter(resource, keyPair.getPublic(), clock, transport);
        }
    }
}
