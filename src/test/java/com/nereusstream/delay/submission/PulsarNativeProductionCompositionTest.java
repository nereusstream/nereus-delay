package com.nereusstream.delay.submission;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.adapter.PinnedPulsarCommandIngress;
import com.nereusstream.delay.adapter.PinnedPulsarNativeSubmissionAdapter;
import com.nereusstream.delay.adapter.PulsarNativePreparedRecordValidator;
import com.nereusstream.delay.adapter.PulsarNativeSendRequest;
import com.nereusstream.delay.adapter.PulsarSendAckEvidence;
import com.nereusstream.delay.adapter.PulsarSendResult;
import com.nereusstream.delay.adapter.PulsarTargetResource;
import com.nereusstream.delay.assessment.DataResetActivationGate;
import com.nereusstream.delay.assessment.DataResetManifest;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.DeliveryContract;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.HandoffPath;
import com.nereusstream.delay.protocol.HandoffPolicyMode;
import com.nereusstream.delay.protocol.HandoffPolicySnapshot;
import com.nereusstream.delay.protocol.NativeCapabilitySnapshot;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.NativePreparedDelivery;
import com.nereusstream.delay.protocol.NativePreparedRecordBinding;
import com.nereusstream.delay.protocol.NativePreparedRecordContext;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProtocolCapabilityDeclaration;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarMetadata;
import com.nereusstream.delay.protocol.PulsarPreparedRecord;
import com.nereusstream.delay.protocol.PulsarSourceLock;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.SubmissionOutcomeKind;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.route.RouteSnapshotProvider;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.semantic.RouteSelectionHint;
import com.nereusstream.delay.transport.Bytes32;
import com.nereusstream.delay.transport.CommandTransportRegistry;
import com.nereusstream.delay.transport.CredentialBindingKey;
import com.nereusstream.delay.transport.GuardedPulsarCommandTransport;
import com.nereusstream.delay.transport.LocalTransportOwnershipPermit;
import com.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import com.nereusstream.delay.transport.PulsarCommandTransportKey;
import com.nereusstream.delay.transport.TransportOwnershipState;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PulsarNativeProductionCompositionTest {
    @Test
    void defaultCoordinatorReachesOnlyTheActivatedPreparedRecordSender() throws Exception {
        final Fixture fixture = fixture();
        final AtomicInteger legacyNativeSends = new AtomicInteger();
        final AtomicInteger preparedRecordSends = new AtomicInteger();
        final PinnedPulsarCommandIngress.PulsarSendTransport managedSender = request ->
                CompletableFuture.failedFuture(new AssertionError("managed sender selected for native branch"));
        final PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport nativeSender =
                new PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport() {
                    @Override
                    public CompletableFuture<PulsarSendResult> send(final PulsarNativeSendRequest request) {
                        legacyNativeSends.incrementAndGet();
                        return CompletableFuture.failedFuture(new AssertionError("envelope-only sender was used"));
                    }

                    @Override
                    public CompletableFuture<PulsarSendResult> sendPreparedRecord(
                            final PulsarPreparedRecord record, final ArtifactGenerationSet artifacts) {
                        preparedRecordSends.incrementAndGet();
                        assertEquals(fixture.record, record);
                        assertEquals(fixture.artifacts, artifacts);
                        final byte[] producerHash = hash("producer");
                        final PublishEvidence evidence = PulsarSendAckEvidence.publishedRecord(
                                record,
                                artifacts,
                                producerHash,
                                11,
                                12,
                                0,
                                1,
                                3_100,
                                21,
                                22,
                                23,
                                24,
                                hash("send-command"),
                                hash("response-command"));
                        return CompletableFuture.completedFuture(new PulsarSendResult(
                                PulsarSendResult.Disposition.PERSISTED,
                                fixture.target.authenticatedClusterId(),
                                fixture.target.resourceIncarnation(),
                                fixture.target.physicalTopic(),
                                fixture.target.physicalTopicCreationTimestamp(),
                                0,
                                11,
                                12,
                                0,
                                1,
                                false,
                                3_100,
                                0,
                                hash("request-evidence"),
                                evidence.canonicalBytes()));
                    }
                };
        final GuardedPulsarCommandTransport transport =
                new GuardedPulsarCommandTransport(fixture.transportKey, managedSender, nativeSender, fixture.validator);
        final CommandTransportRegistry transports = key -> fixture.transportKey.equals(key) ? transport : null;
        final DefaultSubmissionCoordinator coordinator = new DefaultSubmissionCoordinator(
                new RouteBoundSubmissionTransportPlanResolver(routes(), () -> 3_000, fixture.validator),
                transports,
                SubmissionOutcomeProjectorRegistry.of(new PulsarNativeSubmissionOutcomeProjector()));
        final LocalTransportOwnershipPermit permit =
                new LocalTransportOwnershipPermit(PhysicalEnqueueAttemptId.require(bytes(16, 90)));

        final var outcome = coordinator
                .submit(fixture.tenant, fixture.submission, permit)
                .toCompletableFuture()
                .join();

        assertEquals(SubmissionOutcomeKind.NATIVE_RECEIPT, outcome.kind());
        assertEquals(0, legacyNativeSends.get());
        assertEquals(1, preparedRecordSends.get());
        assertEquals(TransportOwnershipState.LIBRARY_OWNED, permit.state());
        assertArrayEquals(
                fixture.prepared.submissionHash(),
                outcome.nativeReceipt().prepared().submissionHash());
    }

    @Test
    void missingActivationValidatorCannotTransferOwnershipOrReachEitherSender() throws Exception {
        final Fixture fixture = fixture();
        final AtomicInteger sends = new AtomicInteger();
        final GuardedPulsarCommandTransport transport = new GuardedPulsarCommandTransport(
                fixture.transportKey,
                request -> {
                    sends.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                },
                request -> {
                    sends.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                });
        final PulsarNativeSendRequest request =
                PulsarNativeSendRequest.from(fixture.resource, fixture.prepared, fixture.record, fixture.artifacts);
        final LocalTransportOwnershipPermit permit =
                new LocalTransportOwnershipPermit(PhysicalEnqueueAttemptId.require(bytes(16, 91)));

        transport.send(request, permit).toCompletableFuture().join();

        assertEquals(0, sends.get());
        assertEquals(TransportOwnershipState.AVAILABLE, permit.state());
    }

    @Test
    void recordContextMutationCannotReuseOneNativePreparedEnvelope() throws Exception {
        final Fixture fixture = fixture();
        final NativePreparedRecordContext different = new NativePreparedRecordContext(
                fixture.context.routeIncarnation(),
                fixture.context.shardPartition(),
                fixture.context.messageId(),
                fixture.context.generation(),
                hash("different-attempt"),
                fixture.context.artifactGenerationSetDigest());

        assertThrows(
                IllegalArgumentException.class, () -> PreparedSubmission.nativePrepared(fixture.prepared, different));
        assertTrue(fixture.submission.isNativeRecordReady());
        assertFalse(fixture.submission.isManaged());
        assertEquals(fixture.submission, PreparedSubmission.decode(fixture.submission.canonicalBytes()));
    }

    private static Fixture fixture() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final ArtifactGenerationSet artifacts =
                ArtifactGenerationSet.current(7, PulsarSourceLock.digest(), hash("schema-bundle"));
        final ShardId shard = new ShardId(new RouteIncarnation(bytes(16, 1)), 3);
        final byte[] tenantScope = hash("tenant");
        final byte[] principalScope = hash("principal");
        final DataResetManifest.ManifestScope manifestScope = new DataResetManifest.ManifestScope(
                "disposable-test", "native-composition", tenantScope, hash("route-snapshot"), new ShardSubject(shard));
        final ProtocolCapabilityDeclaration declaration = new ProtocolCapabilityDeclaration(
                "worker",
                hash("worker"),
                List.of(artifacts.clientCommandTuple(), artifacts.systemMutationTuple()),
                artifacts,
                1,
                hash("session"));
        final DataResetManifest.WorkerCapability worker = new DataResetManifest.WorkerCapability(
                "worker", hash("worker"), hash("session"), declaration, hash("worker-evidence"));
        final DataResetManifest.ResourceIncarnation resourceIncarnation = new DataResetManifest.ResourceIncarnation(
                "PULSAR", "native-topic", hash("resource-incarnation"), true, hash("resource-evidence"));
        final DataResetManifest manifest = DataResetManifest.create(
                manifestScope,
                "0123456789abcdef0123456789abcdef01234567",
                7,
                artifacts,
                List.of(resourceIncarnation),
                hash("fresh-resources"),
                new DataResetManifest.ObligationZeroProof(0, 0, 0, hash("zero-obligations")),
                List.of(worker),
                evidence(1_000),
                new DataResetManifest.ActivationWindow(2_000, 6_000),
                1,
                keys.getPrivate());
        final DataResetActivationGate activationGate =
                new DataResetActivationGate(manifest, keys.getPublic(), "disposable-test", artifacts);
        final PulsarBrokerResourceIdentity target =
                new PulsarBrokerResourceIdentity("cluster", hash("resource"), "persistent://tenant/ns/native", 100);
        final ProfileRef destination =
                new ProfileRef(Bytes.utf8("destination"), 1, hash("destination-semantic"), ProfileKind.DESTINATION);
        final ProfileRef capability = new ProfileRef(
                Bytes.utf8("capability"), 1, hash("capability-semantic"), ProfileKind.DELIVERY_CAPABILITY);
        final TrustedUtcIntervalEvidence issuedAt = evidence(1_200);
        final NativeCapabilitySnapshot capabilitySnapshot = NativeCapabilitySnapshot.create(
                destination,
                capability,
                target,
                0,
                hash("guard"),
                1,
                1,
                hash("credential-binding"),
                hash("credential-fingerprint"),
                principalScope,
                issuedAt,
                6_000,
                1,
                keys.getPrivate());
        final HandoffPolicySnapshot handoff = HandoffPolicySnapshot.create(
                hash("policy-scope"),
                1,
                HandoffPolicyMode.ENABLED,
                100,
                2_000,
                6_000,
                HandoffPath.MANAGED_HANDOFF | HandoffPath.AUTO_FAST,
                issuedAt,
                1,
                artifacts.setDigest(),
                keys.getPrivate());
        final PulsarMetadata metadata =
                new PulsarMetadata(null, null, Bytes.utf8("ordering"), List.of(new PulsarMetadata.Property("a", "b")));
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                destination,
                new RetryPolicyRef(Bytes.utf8("retry"), 1, hash("retry-semantic")),
                5_000,
                7_000,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                Bytes.utf8("payload"),
                null,
                AdapterMetadata.pulsar(metadata),
                null,
                1_500L,
                NativeDeliveryPolicy.ALLOW_AUTO_FAST_AND_MANAGED_HANDOFF);
        final PreparedCommand managed = PreparedCommand.schedule(shard, intent, 8_000);
        final NativePreparedRecordContext context =
                NativePreparedRecordContext.initialSchedule(managed, hash("attempt-seed"), artifacts.setDigest());
        final byte[] nativeDeliveryId = NativePreparedRecordBinding.derive(
                context,
                destination,
                capability,
                target,
                0,
                intent.inlinePayload(),
                metadata,
                intent.eventTimeEpochMs(),
                intent.deliverAtEpochMs(),
                intent.nativeDeliveryPolicy(),
                DeliveryContract.PULSAR_NATIVE_DELIVERY,
                handoff,
                capabilitySnapshot);
        final NativePreparedDelivery prepared = NativePreparedDelivery.createCurrent(
                nativeDeliveryId,
                destination,
                capability,
                target,
                0,
                intent.inlinePayload(),
                metadata,
                intent.eventTimeEpochMs(),
                intent.deliverAtEpochMs(),
                intent.nativeDeliveryPolicy(),
                DeliveryContract.PULSAR_NATIVE_DELIVERY,
                handoff,
                capabilitySnapshot);
        final PreparedSubmission submission = PreparedSubmission.nativePrepared(prepared, context);
        final PulsarTargetResource resource = new PulsarTargetResource(
                target.authenticatedClusterId(),
                target.resourceIncarnation(),
                target.physicalTopic(),
                target.physicalTopicCreationTimestamp(),
                0);
        final PulsarNativePreparedRecordValidator validator = new PulsarNativePreparedRecordValidator(
                resource,
                keys.getPublic(),
                Clock.fixed(Instant.ofEpochMilli(3_000), ZoneOffset.UTC),
                null,
                activationGate);
        final PulsarPreparedRecord record = validator.materialize(submission);
        final PulsarCommandTransportKey transportKey = new PulsarCommandTransportKey(
                target.authenticatedClusterId(),
                target.physicalTopic(),
                new Bytes32(target.resourceIncarnation()),
                target.physicalTopicCreationTimestamp(),
                0,
                new CredentialBindingKey(
                        capabilitySnapshot.credentialBindingGeneration(),
                        new com.nereusstream.delay.transport.Digest32(capabilitySnapshot.credentialBindingDigest()),
                        new com.nereusstream.delay.transport.Digest32(
                                capabilitySnapshot.resolvedCredentialFingerprintDigest())));
        return new Fixture(
                artifacts,
                target,
                resource,
                new AuthenticatedTenantContext(tenantScope, hash("routing"), principalScope),
                context,
                prepared,
                submission,
                validator,
                record,
                transportKey);
    }

    private static RouteSnapshotProvider routes() {
        return new RouteSnapshotProvider() {
            @Override
            public com.nereusstream.delay.protocol.RouteSnapshot activeForNewSchedule(
                    final AuthenticatedTenantContext context, final RouteSelectionHint hint) {
                return null;
            }

            @Override
            public com.nereusstream.delay.protocol.RouteSnapshot exact(
                    final RouteIncarnation incarnation, final AuthenticatedTenantContext context) {
                return null;
            }

            @Override
            public long publishedRevision() {
                return 1;
            }
        };
    }

    private static TrustedUtcIntervalEvidence evidence(final long time) {
        return new TrustedUtcIntervalEvidence(
                time,
                time + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("clock-" + time),
                1,
                time,
                time,
                hash("time-" + time),
                0,
                null);
    }

    private static byte[] hash(final String value) {
        return Bytes.sha256(Bytes.utf8(value));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private record Fixture(
            ArtifactGenerationSet artifacts,
            PulsarBrokerResourceIdentity target,
            PulsarTargetResource resource,
            AuthenticatedTenantContext tenant,
            NativePreparedRecordContext context,
            NativePreparedDelivery prepared,
            PreparedSubmission submission,
            PulsarNativePreparedRecordValidator validator,
            PulsarPreparedRecord record,
            PulsarCommandTransportKey transportKey) {}
}
