package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter;
import com.nereusstream.delay.adapter.DestinationPhysicalAdmission;
import com.nereusstream.delay.adapter.DestinationPublishAdapter;
import com.nereusstream.delay.adapter.DestinationPublishRequest;
import com.nereusstream.delay.adapter.DestinationPublishResult;
import com.nereusstream.delay.adapter.PulsarAttemptJournal;
import com.nereusstream.delay.adapter.PulsarJournalResource;
import com.nereusstream.delay.adapter.PulsarTargetResource;
import com.nereusstream.delay.assessment.DeploymentSafetyGate;
import com.nereusstream.delay.assessment.DisposableEnvironmentAttestation;
import com.nereusstream.delay.assessment.PhysicalSendActivationGate;
import com.nereusstream.delay.protocol.ActivationBarrier;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.ChannelKind;
import com.nereusstream.delay.protocol.ChannelResourceIdentity;
import com.nereusstream.delay.protocol.ClaimMaterialization;
import com.nereusstream.delay.protocol.CredentialUseKind;
import com.nereusstream.delay.protocol.CredentialUseLease;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryContract;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.EvidenceCursor;
import com.nereusstream.delay.protocol.HandoffPath;
import com.nereusstream.delay.protocol.HandoffPolicyHeadRef;
import com.nereusstream.delay.protocol.HandoffPolicyMode;
import com.nereusstream.delay.protocol.HandoffPolicySnapshot;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PayloadForPublish;
import com.nereusstream.delay.protocol.PreparedPublishDescriptor;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarKey;
import com.nereusstream.delay.protocol.PulsarMetadata;
import com.nereusstream.delay.protocol.PulsarPreparedRecord;
import com.nereusstream.delay.protocol.PulsarRecordTemplate;
import com.nereusstream.delay.protocol.PulsarSourceLock;
import com.nereusstream.delay.protocol.ReadyCertificate;
import com.nereusstream.delay.protocol.ReservedPublishMetadata;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.PublishAttemptLedger;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import java.io.ByteArrayOutputStream;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.EnumMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class WorkerManagedPulsarNativePublishTest {
    @Test
    void frozenLeaseIsCheckedBeforeOwnershipAndNativeRecordReachesThePreparedTransport() throws Exception {
        final Fixture fixture = Fixture.create();
        final AtomicInteger preparedCalls = new AtomicInteger();
        final AtomicInteger leaseChecks = new AtomicInteger();
        final AtomicReference<PulsarPreparedRecord> sent = new AtomicReference<>();
        final DestinationPublishAdapter adapter = preparedAdapter(preparedCalls, sent);
        final WorkerPhysicalPublishExecutor executor =
                fixture.executor(adapter, leaseChecks, fixture.validPhysicalTime);

        try (executor) {
            final WorkerPhysicalPublishExecutor.Submission submission = executor.submit(
                    fixture.attempt,
                    WorkerPhysicalPublishExecutor.prepareRequest(fixture.attempt, fixture.payload),
                    () -> 1_950);

            assertEquals(WorkerPhysicalPublishExecutor.SubmissionState.OUTCOME_HANDOFF_QUEUED, submission.state());
            assertEquals(1, preparedCalls.get());
            assertEquals(2, leaseChecks.get());
            assertEquals(
                    DestinationPublishResult.Disposition.PUBLISHED,
                    submission.physicalResult().orElseThrow().disposition());
            assertEquals(
                    DeliveryContract.PULSAR_NATIVE_DELIVERY,
                    sent.get().template().deliveryContract());
            assertEquals(fixture.deliverAt, sent.get().template().nativeDeliverAtEpochMs());
            assertTrue(sent.get().sequenceAuthority().isManagedJournal());
            assertEquals(0, sent.get().sequenceAuthority().sequenceId());
            assertEquals(
                    List.of(
                            PulsarAttemptJournal.RecordKind.MAPPED,
                            PulsarAttemptJournal.RecordKind.OWNERSHIP_STARTED,
                            PulsarAttemptJournal.RecordKind.PUBLISHED),
                    fixture.journal.records().stream()
                            .map(PulsarAttemptJournal.JournalRecord::kind)
                            .toList());
            assertEquals(0, fixture.admission.workerSnapshot().activeRequests());
        }
    }

    @Test
    void expiredFrozenLeaseRetiresMappingWithoutProducerOwnershipOrTargetCall() throws Exception {
        final Fixture fixture = Fixture.create();
        final AtomicInteger preparedCalls = new AtomicInteger();
        final AtomicInteger leaseChecks = new AtomicInteger();
        final AtomicReference<PulsarPreparedRecord> sent = new AtomicReference<>();
        final WorkerPhysicalPublishExecutor executor =
                fixture.executor(preparedAdapter(preparedCalls, sent), leaseChecks, exactTime(3_000));

        try (executor) {
            final WorkerPhysicalPublishExecutor.Submission submission = executor.submit(
                    fixture.attempt,
                    WorkerPhysicalPublishExecutor.prepareRequest(fixture.attempt, fixture.payload),
                    () -> 3_000);

            assertEquals(WorkerPhysicalPublishExecutor.SubmissionState.OUTCOME_HANDOFF_QUEUED, submission.state());
            assertEquals(0, preparedCalls.get());
            assertEquals(0, leaseChecks.get());
            assertTrue(sent.get() == null);
            assertEquals(
                    DestinationPublishResult.Disposition.DEFINITIVELY_NOT_PUBLISHED,
                    submission.physicalResult().orElseThrow().disposition());
            assertEquals(
                    StableCode.CAPABILITY_UNAVAILABLE,
                    submission.physicalResult().orElseThrow().stableCode());
            assertEquals(
                    List.of(
                            PulsarAttemptJournal.RecordKind.MAPPED,
                            PulsarAttemptJournal.RecordKind.RETIRED_NOT_PUBLISHED),
                    fixture.journal.records().stream()
                            .map(PulsarAttemptJournal.JournalRecord::kind)
                            .toList());
            assertEquals(0, fixture.admission.workerSnapshot().activeRequests());
        }
    }

    @Test
    void physicalTimeRegressionBehindAdmissionRetiresBeforeProducerOwnership() throws Exception {
        final Fixture fixture = Fixture.create();
        final AtomicInteger preparedCalls = new AtomicInteger();
        final AtomicInteger leaseChecks = new AtomicInteger();
        final AtomicReference<PulsarPreparedRecord> sent = new AtomicReference<>();
        final WorkerPhysicalPublishExecutor executor =
                fixture.executor(preparedAdapter(preparedCalls, sent), leaseChecks, exactTime(1_899));

        try (executor) {
            final WorkerPhysicalPublishExecutor.Submission submission = executor.submit(
                    fixture.attempt,
                    WorkerPhysicalPublishExecutor.prepareRequest(fixture.attempt, fixture.payload),
                    () -> 1_899);

            assertEquals(WorkerPhysicalPublishExecutor.SubmissionState.OUTCOME_HANDOFF_QUEUED, submission.state());
            assertEquals(0, preparedCalls.get());
            assertEquals(0, leaseChecks.get());
            assertEquals(
                    DestinationPublishResult.Disposition.DEFINITIVELY_NOT_PUBLISHED,
                    submission.physicalResult().orElseThrow().disposition());
            assertEquals(
                    List.of(
                            PulsarAttemptJournal.RecordKind.MAPPED,
                            PulsarAttemptJournal.RecordKind.RETIRED_NOT_PUBLISHED),
                    fixture.journal.records().stream()
                            .map(PulsarAttemptJournal.JournalRecord::kind)
                            .toList());
        }
    }

    @Test
    void missingPhysicalActivationAuthorityBlocksBeforeJournalOrProducerOwnership() throws Exception {
        final Fixture fixture = Fixture.create();
        final AtomicInteger preparedCalls = new AtomicInteger();
        final AtomicInteger leaseChecks = new AtomicInteger();
        final AtomicReference<PulsarPreparedRecord> sent = new AtomicReference<>();
        final WorkerPhysicalPublishExecutor executor =
                fixture.executor(preparedAdapter(preparedCalls, sent), leaseChecks, fixture.validPhysicalTime, null);

        try (executor) {
            final WorkerPhysicalPublishExecutor.Submission submission = executor.submit(
                    fixture.attempt,
                    WorkerPhysicalPublishExecutor.prepareRequest(fixture.attempt, fixture.payload),
                    () -> 1_950);

            assertEquals(WorkerPhysicalPublishExecutor.SubmissionState.OUTCOME_HANDOFF_QUEUED, submission.state());
            assertEquals(0, preparedCalls.get());
            assertEquals(0, leaseChecks.get());
            assertTrue(sent.get() == null);
            assertTrue(fixture.journal.records().isEmpty());
            assertEquals(
                    DestinationPublishResult.Disposition.DEFINITIVELY_NOT_PUBLISHED,
                    submission.physicalResult().orElseThrow().disposition());
            assertEquals(
                    StableCode.CAPABILITY_UNAVAILABLE,
                    submission.physicalResult().orElseThrow().stableCode());
        }
    }

    private static DestinationPublishAdapter preparedAdapter(
            final AtomicInteger calls, final AtomicReference<PulsarPreparedRecord> sent) {
        return new DestinationPublishAdapter() {
            @Override
            public java.util.concurrent.CompletionStage<DestinationPublishResult> publish(
                    final DestinationPublishRequest request) {
                throw new AssertionError("managed Pulsar path used the payload-only transport boundary");
            }

            @Override
            public java.util.concurrent.CompletionStage<DestinationPublishResult> publishPreparedRecord(
                    final PulsarPreparedRecord record, final ArtifactGenerationSet artifacts) {
                calls.incrementAndGet();
                sent.set(record);
                return CompletableFuture.completedFuture(
                        DestinationPublishResult.published(Bytes.utf8("delivery"), 1_951, Bytes.utf8("ack")));
            }
        };
    }

    private static TrustedUtcIntervalEvidence exactTime(final long epochMs) {
        return new TrustedUtcIntervalEvidence(
                epochMs,
                epochMs,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("worker-native-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("worker-native-time-" + epochMs)),
                0,
                null);
    }

    private static final class Fixture {
        private final long deliverAt = 2_000;
        private final byte[] payload = Bytes.utf8("worker-native-payload");
        private final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        private final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("worker-native-lane"));
        private final byte[] laneIncarnation = new byte[16];
        private final byte[] storeIncarnation = new byte[16];
        private final DelayMessageId message = DelayMessageId.random(shard);
        private final byte[] claimId = Bytes.sha256(Bytes.utf8("worker-native-claim"));
        private final byte[] attemptId = SystemMutation.computePublishAttemptLogicalIdentity(claimId, message, 0, 1);
        private final OwnerIdentity owner = new OwnerIdentity(
                Bytes.utf8("worker-native-deployment"),
                Bytes.utf8("worker-native-worker"),
                7,
                Bytes.sha256(Bytes.utf8("worker-native-fence")));
        private final ArtifactGenerationSet artifacts = ArtifactGenerationSet.current(
                1, PulsarSourceLock.digest(), Bytes.sha256(Bytes.utf8("worker-native-schema")));
        private final PhysicalSendActivationGate physicalActivation = PhysicalSendActivationGate.disposableLocal(
                DeploymentSafetyGate.GateBStatus.PASS,
                new DisposableEnvironmentAttestation(
                        "worker-native-disposable",
                        "worker-native-test",
                        true,
                        true,
                        true,
                        true,
                        Bytes.sha256(Bytes.utf8("worker-native-attestation"))),
                artifacts);
        private final PulsarTargetResource target = new PulsarTargetResource(
                "worker-native-cluster",
                Bytes.sha256(Bytes.utf8("worker-native-target-resource")),
                "persistent://tenant/ns/worker-native-target-partition-0",
                17,
                0);
        private final BrokerResourceIdentity targetIdentity =
                BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                        target.authenticatedClusterId(),
                        target.resourceIncarnation(),
                        target.physicalTopic(),
                        target.physicalTopicCreationTimestamp()));
        private final PulsarJournalResource journalResource = new PulsarJournalResource(
                "worker-native-cluster",
                Bytes.sha256(Bytes.utf8("worker-native-journal-resource")),
                "persistent://tenant/ns/worker-native-journal-partition-3",
                19,
                shard.partition());
        private final PulsarAttemptJournal journal = new PulsarAttemptJournal(
                shard,
                request -> new PulsarAttemptJournal.JournalPosition(
                        9,
                        request.kind().ordinal(),
                        0,
                        1,
                        1_900 + request.kind().ordinal()),
                journalResource);
        private final byte[] producerIdentity = Bytes.utf8("worker-native-stable-producer");
        private final PulsarAttemptJournal.ProducerKey producer =
                new PulsarAttemptJournal.ProducerKey(lane, laneIncarnation, Bytes.sha256(producerIdentity), target);
        private final DestinationPhysicalAdmission admission = admission(lane, laneIncarnation);
        private final TrustedUtcIntervalEvidence validPhysicalTime = exactTime(1_950);
        private final PreparedPublishDescriptor descriptor;
        private final PublishAttemptLedger attempt;

        private Fixture() throws Exception {
            final ProfileRef destination = profile(ProfileKind.DESTINATION, "worker-native-destination");
            final ProfileRef capability = profile(ProfileKind.DELIVERY_CAPABILITY, "worker-native-capability");
            final ChannelResourceIdentity channel = channel(destination);
            final HandoffPolicySnapshot snapshot = HandoffPolicySnapshot.create(
                    Bytes.sha256(Bytes.utf8("worker-native-policy-scope")),
                    1,
                    HandoffPolicyMode.ENABLED,
                    100,
                    1_000,
                    3_000,
                    HandoffPath.MANAGED_HANDOFF,
                    exactTime(900),
                    1,
                    artifacts.setDigest(),
                    KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPrivate());
            final HandoffPolicyHeadRef headRef =
                    new HandoffPolicyHeadRef(snapshot.policyScopeDigest(), 1, snapshot.snapshotDigest(), 11);
            final ReservedPublishMetadata reserved = new ReservedPublishMetadata(
                    shard.routeIncarnation(),
                    shard.unsignedPartition(),
                    message,
                    0,
                    attemptId,
                    destination.semanticHash(),
                    capability.semanticHash(),
                    deliverAt,
                    DeliveryMode.MANAGED);
            final PayloadForPublish payloadProjection = PayloadForPublish.inline(payload);
            final AdapterMetadata metadata = AdapterMetadata.pulsar(new PulsarMetadata(null, null, null, List.of()));
            final PulsarRecordTemplate template = new PulsarRecordTemplate(
                    targetIdentity,
                    0,
                    PulsarKey.none(),
                    null,
                    List.of(),
                    777L,
                    reserved,
                    DeliveryContract.PULSAR_NATIVE_DELIVERY,
                    deliverAt,
                    payloadProjection,
                    artifacts.setDigest());
            descriptor = new PreparedPublishDescriptor(
                    AdapterKind.PULSAR,
                    lane,
                    laneIncarnation,
                    destination,
                    capability,
                    targetIdentity,
                    0,
                    channel,
                    message,
                    0,
                    attemptId,
                    1,
                    payloadProjection,
                    metadata,
                    reserved,
                    deliverAt,
                    8_000,
                    1_900,
                    NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF,
                    DeliveryContract.PULSAR_NATIVE_DELIVERY,
                    snapshot,
                    777L,
                    template,
                    template.recordTemplateHash(),
                    artifacts.setDigest());
            final byte[] claimPrecondition = claimPrecondition(descriptor.materialization(headRef));
            final ReadyCertificate certificate = certificate(channel);
            final byte[] admissionBody = PublishAdmissionBody.canonicalBytes(
                    shard,
                    5_000,
                    owner,
                    storeIncarnation,
                    claimId,
                    lane,
                    laneIncarnation,
                    message,
                    0,
                    attemptId,
                    descriptor,
                    charge(),
                    certificate,
                    exactTime(1_900),
                    claimPrecondition);
            final KafkaSourcePosition source = new KafkaSourcePosition(
                    shard,
                    "worker-native-source",
                    UUID.nameUUIDFromBytes(Bytes.utf8("worker-native-source-topic")),
                    12,
                    null,
                    1_900);
            attempt = PublishAttemptLedger.publishing(
                    message,
                    0,
                    attemptId,
                    claimId,
                    owner.ownerEpoch(),
                    1,
                    lane,
                    laneIncarnation,
                    owner.canonicalBytes(),
                    storeIncarnation,
                    descriptor.preparedPublishHash(),
                    admissionBody,
                    source.canonicalBytes());
        }

        private static Fixture create() throws Exception {
            return new Fixture();
        }

        private WorkerPhysicalPublishExecutor executor(
                final DestinationPublishAdapter adapter,
                final AtomicInteger leaseChecks,
                final TrustedUtcIntervalEvidence physicalTime) {
            return executor(adapter, leaseChecks, physicalTime, physicalActivation);
        }

        private WorkerPhysicalPublishExecutor executor(
                final DestinationPublishAdapter adapter,
                final AtomicInteger leaseChecks,
                final TrustedUtcIntervalEvidence physicalTime,
                final PhysicalSendActivationGate activationGate) {
            final WorkerPhysicalPublishExecutor result = new WorkerPhysicalPublishExecutor(
                    new BoundedDestinationPublishAdapter(adapter, admission, workClasses(), Runnable::run),
                    (mutation, ownerClock) -> {},
                    (ignoredAttempt, ignoredRequest, ignoredClock) -> WorkerPhysicalPublishExecutor.Decision.allowed(),
                    (ignoredAttempt, ignoredRequest, ignoredResult) -> mutation(shard),
                    () -> {},
                    activationGate);
            result.bindManagedPulsarContext(new WorkerPhysicalPublishExecutor.ManagedPulsarContext(
                    journal,
                    producer,
                    artifacts,
                    ignored -> {},
                    new WorkerPhysicalPublishExecutor.JournalProjectionSink() {
                        @Override
                        public void recordMapped(
                                final PublishAttemptLedger ignoredAttempt,
                                final long ignoredSequence,
                                final byte[] ignoredPosition) {}

                        @Override
                        public void markRetirementPending(final PublishAttemptLedger ignoredAttempt) {}

                        @Override
                        public void recordRetired(
                                final PublishAttemptLedger ignoredAttempt, final byte[] ignoredPosition) {}
                    },
                    (ignoredAdmission, ignoredArtifacts, ignoredTime, ignoredPosition) -> {
                        assertEquals(
                                PulsarAttemptJournal.AttemptState.MAPPED,
                                journal.state(
                                        journal.records().getFirst().mapping().mappingId()));
                        leaseChecks.incrementAndGet();
                    },
                    () -> physicalTime,
                    owner.ownerEpoch()));
            return result;
        }

        private ChannelResourceIdentity channel(final ProfileRef destination) {
            final BrokerResourceIdentity evidenceIdentity =
                    BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                            journalResource.authenticatedClusterId(),
                            journalResource.resourceIncarnation(),
                            journalResource.physicalTopic(),
                            journalResource.physicalTopicCreationTimestamp()));
            final byte[] guard = Bytes.sha256(Bytes.utf8("worker-native-resource-guard"));
            final byte[] binding = Bytes.sha256(Bytes.utf8("worker-native-binding"));
            final byte[] fingerprint = Bytes.sha256(Bytes.utf8("worker-native-fingerprint"));
            final byte[] prefix = CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.uint32(output, 1, AdapterKind.PULSAR.wireValue());
                CanonicalProtobuf.uint32(output, 2, ChannelKind.PULSAR_DEDUP_PRODUCER.wireValue());
                CanonicalProtobuf.bytes(output, 3, lane.bytes());
                CanonicalProtobuf.bytes(output, 4, laneIncarnation);
                CanonicalProtobuf.bytes(output, 5, targetIdentity.canonicalBytes());
                CanonicalProtobuf.uint32(output, 6, 0);
                CanonicalProtobuf.uint64(output, 7, 1);
                CanonicalProtobuf.uint32(output, 8, 0);
                CanonicalProtobuf.bytes(output, 9, producerIdentity);
                CanonicalProtobuf.bytes(output, 10, Bytes.sha256(producerIdentity));
                CanonicalProtobuf.bytes(output, 11, evidenceIdentity.canonicalBytes());
                CanonicalProtobuf.uint64(output, 12, 1);
                CanonicalProtobuf.bytes(output, 13, guard);
            });
            final CredentialUseLease credential = new CredentialUseLease(
                    destination,
                    CredentialUseKind.DESTINATION_CHANNEL,
                    CredentialUseLease.destinationChannelHolderScope(prefix),
                    1,
                    binding,
                    fingerprint,
                    exactTime(900),
                    9_000,
                    1);
            return new ChannelResourceIdentity(
                    AdapterKind.PULSAR,
                    ChannelKind.PULSAR_DEDUP_PRODUCER,
                    lane.bytes(),
                    laneIncarnation,
                    targetIdentity,
                    0,
                    1,
                    0,
                    producerIdentity,
                    Bytes.sha256(producerIdentity),
                    evidenceIdentity,
                    1L,
                    guard,
                    1,
                    binding,
                    fingerprint,
                    credential);
        }

        private byte[] claimPrecondition(final ClaimMaterialization materialization) {
            final byte[] charge = charge().canonicalBytes();
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, claimId);
                CanonicalProtobuf.bytes(output, 2, message.bytes());
                CanonicalProtobuf.uint32(output, 3, 0);
                CanonicalProtobuf.int64(output, 4, 1);
                CanonicalProtobuf.bytes(output, 5, lane.bytes());
                CanonicalProtobuf.bytes(output, 6, laneIncarnation);
                CanonicalProtobuf.int64(output, 7, 1);
                CanonicalProtobuf.int64(output, 8, 1);
                CanonicalProtobuf.bytes(output, 9, Bytes.sha256(Bytes.utf8("worker-native-timeline")));
                CanonicalProtobuf.bytes(output, 10, materialization.canonicalBytes());
                CanonicalProtobuf.bytes(
                        output,
                        11,
                        Bytes.sha256(
                                Bytes.utf8("nereus-delay-claim-materialization\0"), materialization.canonicalBytes()));
                CanonicalProtobuf.bytes(output, 12, charge);
                CanonicalProtobuf.int64(output, 13, 8_000);
                CanonicalProtobuf.bytes(output, 14, owner.canonicalBytes());
                CanonicalProtobuf.bytes(output, 15, storeIncarnation);
                CanonicalProtobuf.uint32(output, 16, 1);
                CanonicalProtobuf.uint32(output, 17, 0);
                CanonicalProtobuf.uint32(output, 18, 0);
                CanonicalProtobuf.bytes(output, 19, Bytes.sha256(Bytes.utf8("worker-native-obligations")));
                CanonicalProtobuf.bytes(output, 20, Bytes.sha256(Bytes.utf8("worker-native-semantic")));
            });
        }

        private ReadyCertificate certificate(final ChannelResourceIdentity channel) {
            final byte[] guard = channel.resourceGuardAttestationDigest();
            final byte[] barrier = ActivationBarrier.pulsar(targetIdentity, 0, 1, 1, 0, 1, 1, guard)
                    .canonicalBytes();
            final byte[] cursor = EvidenceCursor.pulsar(
                            lane.bytes(),
                            laneIncarnation,
                            journalResource.resourceIncarnation(),
                            shard.partition(),
                            1,
                            1_900,
                            journalResource.physicalTopic(),
                            journalResource.physicalTopicCreationTimestamp(),
                            9,
                            0,
                            0,
                            1)
                    .canonicalBytes();
            final TrustedUtcIntervalEvidence issuedAt = exactTime(1_000);
            final byte[] prefix = CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.uint32(output, 1, 1);
                CanonicalProtobuf.bytes(output, 2, owner.canonicalBytes());
                CanonicalProtobuf.bytes(output, 3, storeIncarnation);
                CanonicalProtobuf.bytes(output, 4, lane.bytes());
                CanonicalProtobuf.bytes(output, 5, laneIncarnation);
                CanonicalProtobuf.bytes(output, 6, channel.canonicalBytes());
                CanonicalProtobuf.bytes(output, 7, barrier);
                CanonicalProtobuf.bytes(output, 8, cursor);
                CanonicalProtobuf.uint32(output, 9, 1);
                CanonicalProtobuf.uint32(output, 10, 1);
                CanonicalProtobuf.int64(output, 11, 7_000);
                CanonicalProtobuf.bytes(output, 12, issuedAt.canonicalBytes());
                CanonicalProtobuf.uint64(output, 13, channel.credentialBindingGeneration());
                CanonicalProtobuf.bytes(output, 14, channel.credentialBindingDigest());
                CanonicalProtobuf.bytes(output, 15, channel.resolvedCredentialVersionFingerprintDigest());
            });
            final byte[] encoded = CanonicalProtobuf.message(output -> {
                final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(prefix);
                while (reader.hasRemaining()) {
                    writeField(output, reader.next());
                }
                CanonicalProtobuf.bytes(
                        output, 16, Bytes.sha256(Bytes.utf8("nereus-delay-ready-certificate\0"), prefix));
            });
            return ReadyCertificate.decode(encoded);
        }

        private static PublishAdmissionBody.ChargeVector charge() {
            return new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 1, 21, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        }

        private static ProfileRef profile(final ProfileKind kind, final String value) {
            return new ProfileRef(Bytes.utf8(value), 1, Bytes.sha256(Bytes.utf8(value + "-semantic")), kind);
        }

        private static SystemMutation mutation(final ShardId shard) {
            try {
                final byte[] logical = Bytes.sha256(Bytes.utf8("worker-native-outcome"));
                final byte[] body = CanonicalProtobuf.message(output -> {
                    CanonicalProtobuf.bytes(output, 1, CanonicalProtobuf.message(subject -> {
                        CanonicalProtobuf.bytes(
                                subject, 1, shard.routeIncarnation().bytes());
                        CanonicalProtobuf.uint32(subject, 2, shard.partition());
                    }));
                    CanonicalProtobuf.uint32(output, 2, SystemMutationType.PUBLISH_OUTCOME.wireValue());
                    CanonicalProtobuf.int64(output, 3, 9_000);
                    CanonicalProtobuf.bytes(output, 10, logical);
                    CanonicalProtobuf.uint32(output, 11, 3);
                    CanonicalProtobuf.uint32(output, 12, 4);
                    CanonicalProtobuf.uint32(output, 13, StableCode.DESTINATION_OUTCOME_UNKNOWN.wireValue());
                    final byte[] nested = CanonicalProtobuf.message(inner -> CanonicalProtobuf.uint32(inner, 1, 1));
                    CanonicalProtobuf.bytes(output, 15, nested);
                    CanonicalProtobuf.bytes(output, 16, nested);
                    CanonicalProtobuf.bytes(output, 17, nested);
                });
                final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
                return SystemMutation.signed(
                        shard,
                        SystemMutationType.PUBLISH_OUTCOME,
                        9_000,
                        logical,
                        body,
                        com.nereusstream.delay.protocol.AuthorIdentity.owner(
                                        Bytes.utf8("deployment"),
                                        Bytes.utf8("worker"),
                                        7,
                                        Bytes.sha256(Bytes.utf8("fence")))
                                .canonicalBytes(),
                        1,
                        keys.getPrivate());
            } catch (java.security.GeneralSecurityException failure) {
                throw new IllegalStateException(failure);
            }
        }

        private static DestinationPhysicalAdmission admission(
                final DestinationLaneId lane, final byte[] laneIncarnation) {
            final DestinationPhysicalAdmission result = new DestinationPhysicalAdmission(1, 10_000);
            result.registerTargetCluster("worker-native-cluster", 1, 10_000);
            result.registerLane(new DestinationPhysicalAdmission.LaneSpec(
                    lane, laneIncarnation, "worker-native-cluster", 0, 0, 1, 10_000, 1, 10_000));
            result.openReady(lane);
            return result;
        }

        private static WorkClassExecutionRegistry workClasses() {
            final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
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

        private static void writeField(final ByteArrayOutputStream output, final CanonicalProtobuf.Reader.Field field) {
            if (field.wireType() == 0) {
                CanonicalProtobuf.uint64Bits(output, field.number(), field.unsignedValue());
            } else {
                CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
            }
        }
    }
}
