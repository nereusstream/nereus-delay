package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.KafkaMetadataV1;
import io.nereusstream.delay.protocol.NativeCapabilitySnapshotV1;
import io.nereusstream.delay.protocol.NativeDeliveryReceiptV1;
import io.nereusstream.delay.protocol.NativePreparedDeliveryV1;
import io.nereusstream.delay.protocol.NonPersistenceProofKindV1;
import io.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.PulsarMetadataV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.RetryPolicyRefV1;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NativeSubmissionAdapterTest {
    @Test
    void persistedResultBecomesNativeReceiptWithPinnedIdentity() throws Exception {
        final Fixture fixture = fixture(4_000, 3_000);
        final byte[] evidence = Bytes.utf8("native-persisted-evidence");
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport = request -> {
            assertEquals(fixture.prepared.canonicalBytes().length, request.preparedBytes().length);
            assertArrayEquals(fixture.prepared.canonicalBytes(), request.preparedBytes());
            return CompletableFuture.completedFuture(PulsarSendResult.persisted(
                    fixture.resource.authenticatedClusterId(), fixture.resource.resourceIncarnation(),
                    fixture.resource.physicalTopic(), fixture.resource.physicalTopicCreationTimestamp(),
                    fixture.resource.partition(), 8, 9, 0, 1, false, 3_100, evidence));
        };
        try (PinnedPulsarNativeSubmissionAdapter adapter = fixture.adapter(transport)) {
            final SubmissionOutcomeMessageV1 outcome = adapter.submit(fixture.prepared, attempt(1))
                    .toCompletableFuture().join();
            assertEquals(io.nereusstream.delay.protocol.SubmissionOutcomeKindV1.NATIVE_RECEIPT, outcome.kind());
            final NativeDeliveryReceiptV1 receipt = outcome.nativeReceipt();
            assertEquals(fixture.prepared.preparedRef(), receipt.prepared());
            assertEquals(0, receipt.brokerAck().partition());
            assertArrayEquals(Bytes.sha256(evidence), receipt.brokerAck().sendReceiptSha256());
            assertEquals(outcome, SubmissionOutcomeMessageV1.decode(outcome.canonicalBytes()));
        }
    }

    @Test
    void guardRejectionBecomesAuthenticatedNativeProof() throws Exception {
        final Fixture fixture = fixture(4_000, 3_000);
        final byte[] evidence = Bytes.utf8("native-guard-rejection");
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport = request ->
                CompletableFuture.completedFuture(PulsarSendResult.definitelyNotPersisted(
                        StableCode.BROKER_DEFINITIVE_NOT_PERSISTED.wireValue(), evidence));
        try (PinnedPulsarNativeSubmissionAdapter adapter = fixture.adapter(transport)) {
            final SubmissionOutcomeMessageV1 outcome = adapter.submit(fixture.prepared, attempt(2))
                    .toCompletableFuture().join();
            assertEquals(io.nereusstream.delay.protocol.SubmissionOutcomeKindV1.NATIVE_DEFINITELY_NOT_QUEUED,
                    outcome.kind());
            assertEquals(NonPersistenceProofKindV1.PULSAR_GUARD_REJECTION,
                    outcome.nativeDefinitelyNotQueued().proof().kind());
            assertEquals(StableCode.NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED,
                    outcome.nativeDefinitelyNotQueued().error().code());
            assertEquals(outcome, SubmissionOutcomeMessageV1.decode(outcome.canonicalBytes()));
        }
    }

    @Test
    void mismatchedDefinitiveCodeCannotBecomeNativeGuardProof() throws Exception {
        final Fixture fixture = fixture(4_000, 3_000);
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport = request ->
                CompletableFuture.completedFuture(PulsarSendResult.definitelyNotPersisted(
                        StableCode.BROKER_RESOURCE_UNCERTIFIED.wireValue(), Bytes.utf8("not-a-guard-rejection")));
        try (PinnedPulsarNativeSubmissionAdapter adapter = fixture.adapter(transport)) {
            final SubmissionOutcomeMessageV1 outcome = adapter.submit(fixture.prepared, attempt(21))
                    .toCompletableFuture().join();
            assertEquals(io.nereusstream.delay.protocol.SubmissionOutcomeKindV1.NATIVE_ENQUEUE_UNCERTAIN,
                    outcome.kind());
            assertEquals(StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN, outcome.nativeUncertain().error().code());
            assertEquals(StableCode.BROKER_RESOURCE_UNCERTIFIED.wireValue(),
                    outcome.nativeUncertain().error().diagnosticCode());
        }
    }

    @Test
    void transportFailureRemainsExactByteUncertain() throws Exception {
        final Fixture fixture = fixture(4_000, 3_000);
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport = request -> {
            throw new IllegalStateException("connection lost after Producer ownership");
        };
        try (PinnedPulsarNativeSubmissionAdapter adapter = fixture.adapter(transport)) {
            final SubmissionOutcomeMessageV1 outcome = adapter.submit(fixture.prepared, attempt(3))
                    .toCompletableFuture().join();
            assertEquals(io.nereusstream.delay.protocol.SubmissionOutcomeKindV1.NATIVE_ENQUEUE_UNCERTAIN,
                    outcome.kind());
            assertEquals(StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN, outcome.nativeUncertain().error().code());
            assertArrayEquals(attempt(3), outcome.nativeUncertain().physicalEnqueueAttemptId());
        }
    }

    @Test
    void callbackRegistrationFailureRemainsExactByteUncertain() throws Exception {
        final Fixture fixture = fixture(4_000, 3_000);
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport = request ->
                new HandleRegistrationFailureFuture<>();
        try (PinnedPulsarNativeSubmissionAdapter adapter = fixture.adapter(transport)) {
            final SubmissionOutcomeMessageV1 outcome = adapter.submit(fixture.prepared, attempt(31))
                    .toCompletableFuture().join();
            assertEquals(io.nereusstream.delay.protocol.SubmissionOutcomeKindV1.NATIVE_ENQUEUE_UNCERTAIN,
                    outcome.kind());
            assertEquals(StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN, outcome.nativeUncertain().error().code());
            assertArrayEquals(attempt(31), outcome.nativeUncertain().physicalEnqueueAttemptId());
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
            final SubmissionOutcomeMessageV1 outcome = adapter.submit(fixture.prepared, attempt(4))
                    .toCompletableFuture().join();
            assertEquals(io.nereusstream.delay.protocol.SubmissionOutcomeKindV1.NATIVE_DEFINITELY_NOT_QUEUED,
                    outcome.kind());
            assertEquals(StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE,
                    outcome.nativeDefinitelyNotQueued().error().code());
        }

        final Fixture expired = fixture(4_000, 4_000);
        try (PinnedPulsarNativeSubmissionAdapter adapter = expired.adapter(transport)) {
            final SubmissionOutcomeMessageV1 outcome = adapter.submit(expired.prepared, attempt(5))
                    .toCompletableFuture().join();
            assertEquals(StableCode.NATIVE_PREPARED_SUBMISSION_EXPIRED,
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
                fixture.resource, fixture.keyPair.getPublic(), fixture.clock, transport,
                prepared -> Bytes.sha256(Bytes.utf8("rotated-credential")))) {
            final SubmissionOutcomeMessageV1 outcome = adapter.submit(fixture.prepared, attempt(41))
                    .toCompletableFuture().join();
            assertEquals(StableCode.CREDENTIAL_BINDING_DRIFT,
                    outcome.nativeDefinitelyNotQueued().error().code());
            assertEquals(NonPersistenceProofKindV1.LOCAL_BEFORE_PRODUCER_OWNERSHIP,
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
                fixture.resource, fixture.keyPair.getPublic(), fixture.clock, transport,
                prepared -> null)) {
            final SubmissionOutcomeMessageV1 outcome = adapter.submit(fixture.prepared, attempt(42))
                    .toCompletableFuture().join();
            assertEquals(StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE,
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
            final SubmissionOutcomeMessageV1 outcome = adapter.submit(fixture.prepared, attempt(43))
                    .toCompletableFuture().join();
            assertEquals(StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE,
                    outcome.nativeDefinitelyNotQueued().error().code());
        }

        final Clock negativeClock = Clock.fixed(Instant.ofEpochMilli(-1), ZoneOffset.UTC);
        try (PinnedPulsarNativeSubmissionAdapter adapter = new PinnedPulsarNativeSubmissionAdapter(
                fixture.resource, fixture.keyPair.getPublic(), negativeClock, transport)) {
            final SubmissionOutcomeMessageV1 outcome = adapter.submit(fixture.prepared, attempt(44))
                    .toCompletableFuture().join();
            assertEquals(StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE,
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
            return CompletableFuture.completedFuture(PulsarSendResult.unknown(
                    StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN.wireValue(), null));
        };
        final PulsarTargetResource mismatched = new PulsarTargetResource(fixture.resource.authenticatedClusterId(),
                Bytes.sha256(Bytes.utf8("different-resource")), fixture.resource.physicalTopic(),
                fixture.resource.physicalTopicCreationTimestamp(), fixture.resource.partition());
        try (PinnedPulsarNativeSubmissionAdapter adapter = new PinnedPulsarNativeSubmissionAdapter(
                mismatched, fixture.keyPair.getPublic(), fixture.clock, transport)) {
            final SubmissionOutcomeMessageV1 outcome = adapter.submit(fixture.prepared, attempt(6))
                    .toCompletableFuture().join();
            assertEquals(StableCode.PREPARED_SUBMISSION_MISMATCH,
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
            public java.util.concurrent.CompletionStage<io.nereusstream.delay.client.EnqueueOutcome> enqueue(
                    final PreparedCommand ignored) {
                return CompletableFuture.failedFuture(new AssertionError("legacy managed path was used"));
            }

            @Override
            public java.util.concurrent.CompletionStage<io.nereusstream.delay.protocol.EnqueueOutcomeMessageV1>
            enqueueOutcomeV1(final PreparedCommand actual, final long queryUntil, final byte[] attemptId) {
                assertEquals(command, actual);
                assertEquals(9_000, queryUntil);
                assertArrayEquals(attempt(7), attemptId);
                return CompletableFuture.completedFuture(WireIngressOutcomeSupport.localDefinite(actual,
                        StableCode.CLIENT_CLOSED));
            }

            @Override
            public void close() {
                closed.set(true);
            }
        };
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport = request ->
                CompletableFuture.failedFuture(new AssertionError("native branch was selected"));
        try (PinnedPulsarNativeSubmissionAdapter nativeAdapter = fixture.adapter(transport);
             PreparedSubmissionAdapter adapter = new PreparedSubmissionAdapter(managed, nativeAdapter)) {
            final SubmissionOutcomeMessageV1 outcome = adapter.submit(
                    io.nereusstream.delay.protocol.PreparedSubmissionV1.managed(CommandCodec.encodeFrameV1(command)),
                    9_000, attempt(7)).toCompletableFuture().join();
            assertEquals(io.nereusstream.delay.protocol.SubmissionOutcomeKindV1.MANAGED, outcome.kind());
            assertEquals(StableCode.CLIENT_CLOSED, outcome.managed().definitelyNotQueued().error().code());
        }
        assertTrue(closed.get());
    }

    @Test
    void preparedSubmissionAdapterFencesManagedSubmissionAfterClose() throws Exception {
        final Fixture fixture = fixture(4_000, 3_000);
        final PreparedCommand command = managedCommand();
        final AtomicBoolean invoked = new AtomicBoolean();
        final WireCommandIngressAdapter managed = new WireCommandIngressAdapter() {
            @Override
            public java.util.concurrent.CompletionStage<io.nereusstream.delay.client.EnqueueOutcome> enqueue(
                    final PreparedCommand ignored) {
                return CompletableFuture.failedFuture(new AssertionError("legacy managed path was used"));
            }

            @Override
            public java.util.concurrent.CompletionStage<io.nereusstream.delay.protocol.EnqueueOutcomeMessageV1>
            enqueueOutcomeV1(final PreparedCommand actual, final long queryUntil, final byte[] attemptId) {
                invoked.set(true);
                return CompletableFuture.completedFuture(WireIngressOutcomeSupport.localDefinite(actual,
                        StableCode.INTEGRITY_ERROR));
            }
        };
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport = request ->
                CompletableFuture.failedFuture(new AssertionError("native branch was selected"));
        try (PinnedPulsarNativeSubmissionAdapter nativeAdapter = fixture.adapter(transport)) {
            final PreparedSubmissionAdapter adapter = new PreparedSubmissionAdapter(managed, nativeAdapter);
            adapter.close();
            final SubmissionOutcomeMessageV1 outcome = adapter.submit(
                    io.nereusstream.delay.protocol.PreparedSubmissionV1.managed(CommandCodec.encodeFrameV1(command)),
                    9_000, attempt(9)).toCompletableFuture().join();
            assertEquals(io.nereusstream.delay.protocol.SubmissionOutcomeKindV1.MANAGED, outcome.kind());
            assertEquals(StableCode.CLIENT_CLOSED, outcome.managed().definitelyNotQueued().error().code());
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
            public java.util.concurrent.CompletionStage<io.nereusstream.delay.client.EnqueueOutcome> enqueue(
                    final PreparedCommand ignored) {
                return CompletableFuture.failedFuture(new AssertionError("legacy managed path was used"));
            }

            @Override
            public java.util.concurrent.CompletionStage<io.nereusstream.delay.protocol.EnqueueOutcomeMessageV1>
            enqueueOutcomeV1(final PreparedCommand actual, final long queryUntil, final byte[] attemptId) {
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
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport = request ->
                CompletableFuture.failedFuture(new AssertionError("native branch was selected"));
        try (PinnedPulsarNativeSubmissionAdapter nativeAdapter = fixture.adapter(transport);
             PreparedSubmissionAdapter adapter = new PreparedSubmissionAdapter(managed, nativeAdapter)) {
            final SubmissionOutcomeMessageV1 outcome = adapter.submit(
                    io.nereusstream.delay.protocol.PreparedSubmissionV1.managed(CommandCodec.encodeFrameV1(command)),
                    9_000, attempt(8)).toCompletableFuture().join();
            assertEquals(io.nereusstream.delay.protocol.SubmissionOutcomeKindV1.MANAGED, outcome.kind());
            assertEquals(StableCode.ENQUEUE_RESULT_UNCERTAIN,
                    outcome.managed().uncertain().error().code());
            assertArrayEquals(attempt(8), outcome.managed().uncertain().physicalEnqueueAttemptId());
        }
        assertTrue(closed.get());
    }

    @Test
    void preparedSubmissionWrapperExceptionalStageRemainsManagedUncertain() throws Exception {
        final Fixture fixture = fixture(4_000, 3_000);
        final PreparedCommand command = managedCommand();
        final AtomicBoolean nativeCalled = new AtomicBoolean();
        final WireCommandIngressAdapter managed = new WireCommandIngressAdapter() {
            @Override
            public java.util.concurrent.CompletionStage<io.nereusstream.delay.client.EnqueueOutcome> enqueue(
                    final PreparedCommand ignored) {
                return CompletableFuture.failedFuture(new AssertionError("legacy managed path was used"));
            }

            @Override
            public java.util.concurrent.CompletionStage<io.nereusstream.delay.protocol.EnqueueOutcomeMessageV1>
            enqueueOutcomeV1(final PreparedCommand actual, final long queryUntil, final byte[] attemptId) {
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
            final SubmissionOutcomeMessageV1 outcome = adapter.submit(
                    io.nereusstream.delay.protocol.PreparedSubmissionV1.managed(CommandCodec.encodeFrameV1(command)),
                    9_000, attempt(10)).toCompletableFuture().join();
            assertEquals(io.nereusstream.delay.protocol.SubmissionOutcomeKindV1.MANAGED, outcome.kind());
            assertEquals(StableCode.ENQUEUE_RESULT_UNCERTAIN,
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
            public java.util.concurrent.CompletionStage<io.nereusstream.delay.client.EnqueueOutcome> enqueue(
                    final PreparedCommand ignored) {
                return CompletableFuture.failedFuture(new AssertionError("legacy managed path was used"));
            }

            @Override
            public java.util.concurrent.CompletionStage<io.nereusstream.delay.protocol.EnqueueOutcomeMessageV1>
            enqueueOutcomeV1(final PreparedCommand actual, final long queryUntil, final byte[] attemptId) {
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
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport = request ->
                CompletableFuture.failedFuture(new AssertionError("native branch was selected"));
        try (PinnedPulsarNativeSubmissionAdapter nativeAdapter = fixture.adapter(transport);
             PreparedSubmissionAdapter adapter = new PreparedSubmissionAdapter(managed, nativeAdapter)) {
            final SubmissionOutcomeMessageV1 outcome = adapter.submit(
                    io.nereusstream.delay.protocol.PreparedSubmissionV1.managed(CommandCodec.encodeFrameV1(command)),
                    9_000, null).toCompletableFuture().join();
            assertEquals(io.nereusstream.delay.protocol.SubmissionOutcomeKindV1.MANAGED, outcome.kind());
            assertEquals(StableCode.INVALID_PREPARED_COMMAND,
                    outcome.managed().definitelyNotQueued().error().code());
            assertEquals(NonPersistenceProofKindV1.LOCAL_BEFORE_PRODUCER_OWNERSHIP,
                    outcome.managed().definitelyNotQueued().proof().kind());
        }
    }

    private static PreparedCommand managedCommand() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 11);
        final ProfileRefV1 destination = new ProfileRefV1(Bytes.utf8("managed-destination"), 1,
                Bytes.sha256(Bytes.utf8("managed-destination-semantic")), ProfileKindV1.DESTINATION);
        final RetryPolicyRefV1 retryPolicy = new RetryPolicyRefV1(Bytes.utf8("managed-retry"), 1,
                Bytes.sha256(Bytes.utf8("managed-retry-semantic")));
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(destination, retryPolicy, 2_000, 5_000,
                DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0], Bytes.utf8("managed-payload"), null,
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, java.util.List.of())), null, null);
        return PreparedCommand.scheduleV1(shard, intent, 8_000);
    }

    private static Fixture fixture(final long expiry, final long now) throws Exception {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final byte[] resourceBytes = nonZero(32, 7);
        final PulsarBrokerResourceIdentityV1 target = new PulsarBrokerResourceIdentityV1(
                "native-cluster", resourceBytes, "persistent://tenant/ns/native", Long.MIN_VALUE);
        final ProfileRefV1 destination = new ProfileRefV1(Bytes.utf8("native-destination"), 1,
                Bytes.sha256(Bytes.utf8("native-destination-semantic")), ProfileKindV1.DESTINATION);
        final ProfileRefV1 capability = new ProfileRefV1(Bytes.utf8("native-capability"), 1,
                Bytes.sha256(Bytes.utf8("native-capability-semantic")), ProfileKindV1.DELIVERY_CAPABILITY);
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(2_100, 2_110,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("native-clock"), 1, 2, 3,
                Bytes.sha256(Bytes.utf8("native-sample")), 0, null);
        final NativeCapabilitySnapshotV1 snapshot = NativeCapabilitySnapshotV1.create(destination, capability, target,
                0, Bytes.sha256(Bytes.utf8("native-guard")), 1, 1, Bytes.sha256(Bytes.utf8("native-binding")),
                Bytes.sha256(Bytes.utf8("native-fingerprint")), Bytes.sha256(Bytes.utf8("native-scope")), issuedAt,
                expiry, 1, keyPair.getPrivate());
        final NativePreparedDeliveryV1 prepared = NativePreparedDeliveryV1.create(nonZero(32, 8), destination,
                capability, target, 0, Bytes.utf8("native-payload"),
                new PulsarMetadataV1(null, null, null, java.util.List.of()), null, 2_200, 2_300, snapshot);
        final PulsarTargetResource resource = new PulsarTargetResource(target.authenticatedClusterId(),
                target.resourceIncarnation(), target.physicalTopic(), target.physicalTopicCreationTimestamp(), 0);
        return new Fixture(keyPair, resource, prepared,
                Clock.fixed(Instant.ofEpochMilli(now), ZoneOffset.UTC));
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
        public <U> CompletableFuture<U> thenApply(
                final java.util.function.Function<? super T, ? extends U> function) {
            throw new IllegalStateException("completion callback registration failed");
        }

        @Override
        public <U> CompletableFuture<U> handle(
                final java.util.function.BiFunction<? super T, Throwable, ? extends U> function) {
            throw new IllegalStateException("completion callback registration failed");
        }
    }

    private record Fixture(KeyPair keyPair, PulsarTargetResource resource, NativePreparedDeliveryV1 prepared,
                           Clock clock) {
        private PinnedPulsarNativeSubmissionAdapter adapter(
                final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport transport) {
            return new PinnedPulsarNativeSubmissionAdapter(resource, keyPair.getPublic(), clock, transport);
        }
    }
}
