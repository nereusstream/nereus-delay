package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter;
import com.nereusstream.delay.adapter.DestinationPhysicalAdmission;
import com.nereusstream.delay.adapter.DestinationPublishAdapter;
import com.nereusstream.delay.adapter.DestinationPublishRequest;
import com.nereusstream.delay.adapter.DestinationPublishResult;
import com.nereusstream.delay.adapter.PulsarPreparedRecordFactory;
import com.nereusstream.delay.runtime.AttemptLedgerState;
import com.nereusstream.delay.runtime.AttemptObligationRef;
import com.nereusstream.delay.runtime.CurrentSendWorkKind;
import com.nereusstream.delay.runtime.GenerationAggregateState;
import com.nereusstream.delay.runtime.TargetClaimRecord;
import com.nereusstream.delay.runtime.TargetGenerationRuntimeIndex;
import com.nereusstream.delay.runtime.TargetMessageRecord;
import com.nereusstream.delay.runtime.TargetPublishAdmissionVerifier;
import com.nereusstream.delay.runtime.TargetTimelineWorkRef;
import com.nereusstream.delay.runtime.TimelineWorkKind;
import com.nereusstream.delay.runtime.UncertainRetryAuthority;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import com.nereusstream.delay.store.KeyCodec;
import java.security.KeyPairGenerator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TargetMaterializedAdmissionTest {
    @Test
    void publicationFreezesExactClaimChannelPayloadAndRecordIdentity() throws Exception {
        final var f = fixture();
        final var publication = publication(f);
        final var decoded = TargetOrdinaryPublicationBinding.decode(publication.canonicalBytes());
        assertEquals(publication, decoded);
        assertArrayEquals(publication.preparedPublishHash(), decoded.preparedPublishHash());
        assertDoesNotThrow(() -> decoded.requireClaim(f.claim(), f.claimed(), f.binding()));
        assertDoesNotThrow(() -> decoded.requirePayload(f.claimed().inlinePayload()));
        assertThrows(IllegalArgumentException.class, () -> decoded.requirePayload(Bytes.utf8("wrong")));
        assertThrows(IllegalStateException.class, () -> decoded.requireClaim(f.claim(), f.initial(), f.binding()));
        final byte[] changed = publication.canonicalBytes();
        changed[changed.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> TargetOrdinaryPublicationBinding.decode(changed));
        assertThrows(IllegalArgumentException.class, () -> TargetOrdinaryPublicationBinding.decode(
                new byte[TargetOrdinaryPublicationBinding.MAX_CANONICAL_BYTES + 1]));
    }

    @Test
    void materializedBodyUsesIndependentTupleAndRequiresResolvedAuthority() throws Exception {
        final var f = fixture();
        final var publication = publication(f);
        final var body = admission(f, publication);
        final var decoded = TargetPublishAdmissionBody.decode(body.canonicalBytes());
        assertEquals(4, decoded.bodyVersion());
        assertEquals(3, TargetPublishAdmissionBody.BODY_VERSION);
        assertEquals(publication, decoded.publication());
        assertArrayEquals(f.claim().canonicalBytes(), decoded.claimProof().canonicalBytes());
        final var keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final var owner = f.claim().owner();
        final var mutation = SystemMutation.signed(
                body.shard(), SystemMutationType.TARGET_PUBLISH_ADMISSION, body.retryUntilEpochMs(),
                body.publishAttemptId(), body.canonicalBytes(),
                AuthorIdentity.owner(owner.deploymentId(), owner.workerRunId(),
                                owner.ownerEpoch(), owner.leaseFencingDigest())
                        .canonicalBytes(),
                1, keys.getPrivate());
        assertArrayEquals(mutation.encodeFrame(), SystemMutation.decodeFrame(mutation.encodeFrame()).encodeFrame());
        final var oldSource = (KafkaSourcePosition) f.binding().bindingSource();
        final var at = new KafkaSourcePosition(oldSource.shardId(), oldSource.authenticatedClusterId(),
                oldSource.nativeTopicUuid(), oldSource.offset() + 1, oldSource.leaderEpoch(),
                body.decisionTime().earliestEpochMs());
        final var scope = new TargetQuotaScope(at.shardId(), repeated(32, 0x51), null);
        final var accepted = TargetPublishAdmissionVerifier.decideFirstApplication(
                scope, mutation, at,
                (a, b, c, d) -> new TargetPublishAdmissionVerifier.Authorization(
                        keys.getPublic(), ProtocolTuple.targetMaterializedPublishAdmission(), 10, 10, 100,
                        (e, g, h, i) -> true, (materialized, source) -> {
                            assertEquals(publication, materialized.publication());
                            assertEquals(at, source);
                        }));
        assertNull(accepted.rejection());
        assertEquals(StableCode.UNAUTHORIZED_SYSTEM_MUTATION,
                TargetPublishAdmissionVerifier.decideFirstApplication(
                                scope, mutation, at,
                                (a, b, c, d) -> new TargetPublishAdmissionVerifier.Authorization(
                                        keys.getPublic(), ProtocolTuple.targetPublishAdmission(),
                                        10, 10, 100, (e, g, h, i) -> true))
                        .rejection());
        assertThrows(IllegalStateException.class, () -> TargetPublishAdmissionVerifier.decideFirstApplication(
                scope, mutation, at,
                (a, b, c, d) -> new TargetPublishAdmissionVerifier.Authorization(
                        keys.getPublic(), ProtocolTuple.targetMaterializedPublishAdmission(),
                        10, 10, 100, (e, g, h, i) -> true)));
    }

    private record Fixture(
            TargetScheduleBinding binding,
            CanonicalTargetPartition physical,
            TargetChannelIdentity channel,
            TargetMessageRecord initial,
            TargetClaimRecord claim,
            TargetMessageRecord claimed) {}

    @Test
    void targetPublishedEvidenceRequiresExactResourcePartitionAndPreparedCommitment() throws Exception {
        final var f = fixture();
        final var publication = publication(f);
        final var body = admission(f, publication);
        final var transfer = PublishAdmissionBody.ChargeVector.decodeCanonical(body.outcomeTransfer());
        assertEquals(1, transfer.inflightMessages());
        assertEquals(body.executionBytes(), transfer.inflightBytes());
        assertEquals(body.commitment().amount(CapacityDimension.RESULT_BYTES), transfer.resultBytes());
        final long part = publication.physical().physicalPartition();
        assertDoesNotThrow(() -> pulsarAck(publication, part, publication.preparedPublishHash())
                .requireOrdinaryTargetPublishedBinding(publication));
        assertThrows(IllegalArgumentException.class, () -> pulsarAck(publication, part + 1,
                publication.preparedPublishHash()).requireOrdinaryTargetPublishedBinding(publication));
        assertThrows(IllegalArgumentException.class, () -> pulsarAck(publication, part, repeated(32, 0x61))
                .requireOrdinaryTargetPublishedBinding(publication));
    }

    private static PublishEvidence pulsarAck(
            TargetOrdinaryPublicationBinding publication, long partition, byte[] preparedHash) {
        final byte[] branch = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, publication.physical().resource().canonicalBytes());
            CanonicalProtobuf.uint32(out, 2, partition);
            CanonicalProtobuf.uint64(out, 3, 1);
            CanonicalProtobuf.uint64(out, 4, 2);
            CanonicalProtobuf.uint32(out, 5, 0);
            CanonicalProtobuf.uint64(out, 6, publication.deliverAtEpochMs());
            CanonicalProtobuf.bytes(out, 7, repeated(32, 0x61));
            CanonicalProtobuf.uint64(out, 8, 1);
            CanonicalProtobuf.bytes(out, 9,
                    ExternalDeliveryIdentity.publishAttempt(publication.publishAttemptId()).canonicalBytes());
            CanonicalProtobuf.bytes(out, 10, preparedHash);
            CanonicalProtobuf.bytes(out, 11, repeated(32, 0x62));
        });
        return PublishEvidence.create(
                PublishEvidenceKind.PULSAR_SEND_ACK, EvidenceVerificationStatus.VERIFIED_PUBLISHED, branch);
    }

    private static Fixture fixture() throws Exception {
        return fixture("binding.best");
    }

    private static Fixture fixture(String bindingKey) throws Exception {
        final var binding = TargetScheduleBinding.decode(vector("target-binding-channel", bindingKey));
        final var physical = CanonicalTargetPartition.decode(vector("target-compatibility", "pulsar.target"));
        final var channel = TargetChannelIdentity.decode(vector("target-binding-channel", "channel.pulsar.journal"));
        final var template = TargetQuotaClaimCharge.decode(vector("target-quota-claim", "native"));
        final var intent = binding.intent();
        final var locator = new TargetMessageLocator(
                binding.messageId(), 0, binding.target(), binding.domain(), binding.accountingIncarnation(),
                intent.orderingMode(), binding.orderingDomain(), binding.digest());
        final var work = new TargetTimelineWorkRef(
                locator, TimelineWorkKind.INITIAL_SCHEDULE, intent.deliverAtEpochMs(), intent.deliverAtEpochMs(),
                binding.bindingSource().sourceOrderToken(), 1, 1, UncertainRetryAuthority.NONE, null, null, false);
        final var initial = new TargetMessageRecord(
                locator, 1, intent.deliverAtEpochMs(), intent.expireAtEpochMs(), intent.deliverAtEpochMs(),
                intent.nativeDeliveryPolicy(), binding.bindingSource(),
                intent.hasInlinePayload() ? intent.inlinePayload() : null,
                intent.hasInlinePayload() ? null : PayloadReference.fromDescriptor(intent.committedPayload()),
                new TargetGenerationRuntimeIndex(0, GenerationAggregateState.SCHEDULED, CurrentSendWorkKind.TIMELINE,
                        work, null, null, List.of(), 0, 0, false, 1));
        final var claim = new TargetClaimRecord(
                initial.runtime(), initial.stateVersion(), initial.digest(),
                new TargetHeadRef(work.ordinaryKey(), locator.messageId(), 0, work.ordinaryEligibilityAtEpochMs()),
                template.owner(), template.storeIncarnation(), 3, intent.expireAtEpochMs() - 1, 128, 1,
                template.creation());
        return new Fixture(binding, physical, channel, initial, claim, claim.claimed(initial));
    }

    private static TargetOrdinaryPublicationBinding publication(Fixture f) {
        return publication(f, repeated(32, 0x42));
    }

    private static TargetOrdinaryPublicationBinding publication(Fixture f, byte[] artifacts) {
        return TargetOrdinaryPublicationBinding.fromClaim(
                f.claim(), f.claimed(), f.binding(), f.physical(), f.channel(),
                new ProfileRef(Bytes.utf8("capability"), 1, repeated(32, 0x41), ProfileKind.DELIVERY_CAPABILITY),
                artifacts);
    }

    @Test
    void targetRecordJoinsFrozenMessageMetadataPayloadAndProducerWithoutLane() throws Exception {
        final var f = fixture();
        final var artifacts = ArtifactGenerationSet.current(1, PulsarSourceLock.digest(), repeated(32, 0x71));
        final var publication = publication(f, artifacts.setDigest());
        final var admitted = admission(f, publication);
        final var message = f.claim().admitted(f.claimed(), admitted.obligation(), admitted.attemptNo());
        final var payload = PayloadForPublish.inline(message.inlinePayload());
        final var sequence = PulsarSequenceAuthority.managedJournal(
                repeated(32, 0x72), 7, Bytes.sha256(f.channel().context().producerIdentity()));
        final var record = PulsarPreparedRecordFactory.targetManaged(
                publication, message, payload, ResolvedPayload.of(message.inlinePayload()), sequence, artifacts);
        assertEquals(record, PulsarPreparedRecord.decode(record.canonicalBytes()));
        assertEquals(DeliveryContract.NEREUS_MANAGED_NOT_BEFORE, record.template().deliveryContract());
        assertNull(record.template().nativeDeliverAtEpochMs());
        assertEquals(publication.adapterMetadata().pulsar().properties(), record.template().callerProperties());
        assertArrayEquals(publication.adapterMetadata().pulsar().orderingKey(), record.template().orderingKey());
        assertEquals(publication.eventTimeEpochMs(), record.template().eventTimeEpochMs());
        assertArrayEquals(publication.preparedPublishHash(), record.preparedIdentityHash());
        assertArrayEquals(message.inlinePayload(), record.resolvedPayload().bytes());
        assertEquals(9, record.finalReservedProperties().size());
        assertThrows(IllegalArgumentException.class, () -> PulsarPreparedRecordFactory.targetManaged(
                publication, f.claimed(), payload, record.resolvedPayload(), sequence, artifacts));
        assertThrows(IllegalArgumentException.class, () -> PulsarPreparedRecordFactory.targetManaged(
                publication, message, PayloadForPublish.inline(Bytes.utf8("wrong")),
                ResolvedPayload.of(Bytes.utf8("wrong")), sequence, artifacts));
        assertThrows(IllegalArgumentException.class, () -> PulsarPreparedRecordFactory.targetManaged(
                publication, message, payload, record.resolvedPayload(),
                PulsarSequenceAuthority.managedJournal(repeated(32, 0x72), 7, repeated(32, 0x73)), artifacts));
    }

    @Test
    void targetRecordPreservesCommittedObjectIdentityAndRejectsForeignProjection() throws Exception {
        final var f = fixture("binding.object");
        final var artifacts = ArtifactGenerationSet.current(1, PulsarSourceLock.digest(), repeated(32, 0x71));
        final var publication = publication(f, artifacts.setDigest());
        final var admitted = admission(f, publication);
        final var message = f.claim().admitted(f.claimed(), admitted.obligation(), admitted.attemptNo());
        final var descriptor = f.binding().intent().committedPayload();
        final var projection = PayloadForPublish.object(descriptor);
        final var resolved = ResolvedPayload.of(Bytes.utf8("payload"));
        final var sequence = PulsarSequenceAuthority.managedJournal(
                repeated(32, 0x72), 7, Bytes.sha256(f.channel().context().producerIdentity()));
        final var record = PulsarPreparedRecordFactory.targetManaged(
                publication, message, projection, resolved, sequence, artifacts);
        assertEquals(descriptor, record.template().payload().object());
        final var foreign = new CommittedPayloadDescriptor(
                descriptor.objectStoreProfile(), descriptor.container(), Bytes.utf8("another-object"),
                descriptor.immutableObjectVersion(), descriptor.etag(), descriptor.length(),
                descriptor.payloadSha256(), descriptor.reservationId(), descriptor.proofId());
        assertThrows(IllegalArgumentException.class, () -> PulsarPreparedRecordFactory.targetManaged(
                publication, message, PayloadForPublish.object(foreign), resolved, sequence, artifacts));
        assertThrows(IllegalArgumentException.class, () -> PulsarPreparedRecordFactory.targetManaged(
                publication, message, PayloadForPublish.inline(resolved.bytes()), resolved, sequence, artifacts));
    }

    @Test
    void targetGenerationsAndSourceShardsSharePhysicalPoolAndRetainOldZombies() throws Exception {
        final var f = fixture();
        final var channel = f.channel();
        final var renewal = channelFor(channel, channel.context().sourceShard(),
                TargetQueueState.nextRevision(channel.context().channelGeneration()));
        renewal.requireSuccessorOf(channel);
        final var other = channelFor(channel,
                new ShardId(channel.context().sourceShard().routeIncarnation(),
                        channel.context().sourceShard().partition() + 1), channel.context().channelGeneration());
        final var pool = new DestinationPhysicalAdmission(2, 100);
        pool.registerTargetCluster(f.physical().resource().pulsar().authenticatedClusterId(), 2, 100);
        for (var candidate : List.of(channel, renewal, other)) {
            pool.registerTargetChannel(new DestinationPhysicalAdmission.TargetChannelSpec(
                    candidate, f.physical(), 2, 100, 2, 100));
            pool.openTargetReady(candidate);
        }
        assertEquals(0, pool.workerSnapshot().protectedReadyRequests());
        final var first = pool.tryAcquireTarget(channel, 30).reservation();
        assertEquals(channel, first.targetChannel());
        assertThrows(IllegalStateException.class, first::laneId);
        assertTrue(first.markZombie());
        pool.closeTargetReady(channel);
        assertThrows(IllegalStateException.class, () -> pool.unregisterTargetChannel(channel));
        final var second = pool.tryAcquireTarget(renewal, 30).reservation();
        assertEquals(DestinationPhysicalAdmission.Rejection.WORKER_CAPACITY,
                pool.tryAcquireTarget(other, 1).rejection());
        assertEquals(1, pool.targetChannelSnapshot(channel).zombieRequests());
        pool.closeTargetSourceShard(channel.context().sourceShard());
        assertFalse(pool.targetChannelSnapshot(renewal).ready());
        assertTrue(pool.targetChannelSnapshot(other).ready());
        second.release();
        final var sibling = pool.tryAcquireTarget(other, 1).reservation();
        assertEquals(2, pool.workerSnapshot().activeRequests());
        first.release();
        assertFalse(first.release());
        pool.unregisterTargetChannel(channel);
        assertEquals(1, pool.workerSnapshot().activeRequests());
        sibling.release();
        assertEquals(0, pool.workerSnapshot().activeBytes());
    }

    @Test
    void boundedTargetRecordRechecksSourceCloseAndRetainsTimedOutPhysicalCharge() throws Exception {
        final var f = fixture();
        final var artifacts = ArtifactGenerationSet.current(1, PulsarSourceLock.digest(), repeated(32, 0x71));
        final var publication = publication(f, artifacts.setDigest());
        final var admitted = admission(f, publication);
        final var message = f.claim().admitted(f.claimed(), admitted.obligation(), admitted.attemptNo());
        final var record = PulsarPreparedRecordFactory.targetManaged(
                publication, message, PayloadForPublish.inline(message.inlinePayload()),
                ResolvedPayload.of(message.inlinePayload()), PulsarSequenceAuthority.managedJournal(
                        repeated(32, 0x72), 7, Bytes.sha256(f.channel().context().producerIdentity())), artifacts);
        final var pool = new DestinationPhysicalAdmission(1, 100_000);
        pool.registerTargetCluster(f.physical().resource().pulsar().authenticatedClusterId(), 1, 100_000);
        pool.registerTargetChannel(new DestinationPhysicalAdmission.TargetChannelSpec(
                f.channel(), f.physical(), 1, 100_000, 1, 100_000));
        pool.openTargetReady(f.channel());
        final var tasks = new ArrayList<Runnable>();
        final var physical = new CompletableFuture<DestinationPublishResult>();
        final var calls = new AtomicInteger();
        final var preflights = new AtomicInteger();
        final DestinationPublishAdapter delegate = new DestinationPublishAdapter() {
            @Override
            public java.util.concurrent.CompletionStage<DestinationPublishResult> publish(
                    DestinationPublishRequest ignored) {
                throw new AssertionError("Target record must not become a Lane request");
            }

            @Override
            public java.util.concurrent.CompletionStage<DestinationPublishResult> publishPreparedRecord(
                    PulsarPreparedRecord actual, ArtifactGenerationSet actualArtifacts,
                    BoundedDestinationPublishAdapter.PreparedPublishPreflight ownershipGate) {
                assertEquals(record, actual);
                assertArrayEquals(artifacts.setDigest(), actualArtifacts.setDigest());
                final var denied = ownershipGate.check(actual, actualArtifacts);
                if (denied != null) {
                    return CompletableFuture.completedFuture(denied);
                }
                calls.incrementAndGet();
                return physical;
            }
        };
        final var classes = workClasses();
        final var adapter = new BoundedDestinationPublishAdapter(delegate, pool, classes, tasks::add);
        final BoundedDestinationPublishAdapter.TargetPreparedPublishPreflight fixtureGate = (p, r, a) -> {
            assertEquals(publication, p);
            assertEquals(record, r);
            preflights.incrementAndGet();
            return null;
        };
        assertThrows(NullPointerException.class,
                () -> adapter.submitTargetPreparedRecord(publication, record, artifacts, null));
        final var legacyRecordCalls = new AtomicInteger();
        final DestinationPublishAdapter legacy = new DestinationPublishAdapter() {
            @Override
            public java.util.concurrent.CompletionStage<DestinationPublishResult> publish(
                    DestinationPublishRequest ignored) {
                throw new AssertionError("Target record must not use ordinary compatibility");
            }

            @Override
            public java.util.concurrent.CompletionStage<DestinationPublishResult> publishPreparedRecord(
                    PulsarPreparedRecord r, ArtifactGenerationSet a) {
                legacyRecordCalls.incrementAndGet();
                return CompletableFuture.completedFuture(
                        DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null));
            }
        };
        final var unsupported = new BoundedDestinationPublishAdapter(legacy, pool, classes, Runnable::run);
        unsupported.submitTargetPreparedRecord(publication, record, artifacts, fixtureGate)
                .outcome().toCompletableFuture().join();
        assertEquals(0, legacyRecordCalls.get());
        assertEquals(0, preflights.get());
        assertEquals(0, pool.workerSnapshot().activeRequests());
        unsupported.close();
        final var stopped = adapter.submitTargetPreparedRecord(publication, record, artifacts, fixtureGate);
        assertEquals(record.physicalByteCharge(), pool.workerSnapshot().activeBytes());
        pool.closeTargetSourceShard(f.channel().context().sourceShard());
        tasks.removeFirst().run();
        assertEquals(0, calls.get());
        assertEquals(0, preflights.get());
        assertEquals(DestinationPublishResult.Disposition.UNKNOWN,
                stopped.outcome().toCompletableFuture().join().disposition());
        assertEquals(0, pool.workerSnapshot().activeRequests());
        pool.openTargetReady(f.channel());
        final var sent = adapter.submitTargetPreparedRecord(publication, record, artifacts, fixtureGate);
        tasks.removeFirst().run();
        assertEquals(1, calls.get());
        assertEquals(1, preflights.get());
        assertTrue(sent.markCallbackTimeout());
        assertEquals(1, pool.targetChannelSnapshot(f.channel()).zombieRequests());
        pool.closeTargetReady(f.channel());
        assertThrows(IllegalStateException.class, () -> pool.unregisterTargetChannel(f.channel()));
        physical.complete(DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null));
        sent.outcome().toCompletableFuture().join();
        assertEquals(0, pool.workerSnapshot().activeRequests());
        pool.unregisterTargetChannel(f.channel());
        adapter.close();
    }

    private static TargetChannelIdentity channelFor(TargetChannelIdentity prior, ShardId shard, long generation) {
        final var c = prior.context();
        final var context = new TargetChannelIdentity.Context(
                shard, c.target(), c.domain(), c.accountingIncarnation(), c.dispatchCompatibilityRef(),
                c.controlScopeRef(), c.kind(), c.channelSlot(), generation, c.evidenceGeneration(),
                c.resourceGuardAttestationDigest());
        final var lease = prior.credentialLease();
        return new TargetChannelIdentity(context, new CredentialUseLease(
                lease.profile(), lease.kind(), context.credentialHolderScope(), lease.secretGeneration(),
                lease.credentialBindingDigest(), lease.resolvedCredentialFingerprintDigest(), lease.issuedAt(),
                lease.validUntilEpochMs(), lease.protectionRevision()));
    }

    private static WorkClassExecutionRegistry workClasses() {
        final var policies = new EnumMap<WorkClass, WorkClassPolicy>(WorkClass.class);
        for (var kind : WorkClass.values()) {
            final boolean protectedClass = kind != WorkClass.QUERY && kind != WorkClass.CHECKPOINT;
            policies.put(kind, new WorkClassPolicy(
                    1, 1, 1_000_000, 1, 1_000_000, 1_000, protectedClass ? 1 : 0,
                    protectedClass ? 8 : 0, kind == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(
                new WorkClassRuntimeConfig(policies, 100, 100, 16, 8_000_000), () -> 0);
    }

    private static TargetPublishAdmissionBody admission(Fixture f, TargetOrdinaryPublicationBinding publication) {
        final long[] reserve = new long[CapacityDimension.COUNT];
        reserve[CapacityDimension.RESULT_BYTES.wireValue() - 1] = 256;
        final var time = new TrustedUtcIntervalEvidence(
                f.claimed().deliverAtEpochMs(), f.claimed().deliverAtEpochMs() + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"),
                1, 1, 1, repeated(32, 0x43), 0, null);
        final var ref = new AttemptObligationRef(publication.publishAttemptId(), publication.locator().generation(),
                AttemptLedgerState.PUBLISHING,
                KeyCodec.inflight((byte) 2, f.claim().owner().ownerEpoch(), publication.publishAttemptId()));
        return new TargetPublishAdmissionBody(
                f.binding().bindingSource().shardId(), time.latestEpochMs() + 10_000, f.claim().owner(),
                f.claim().storeIncarnation(), f.claim().claimId(), publication.locator(), publication.attemptNo(),
                publication.publishAttemptId(), ref, 128, new CapacityVector(reserve), CapacityVector.empty(), time,
                publication, f.claim());
    }

    private static byte[] repeated(int count, int value) {
        final byte[] bytes = new byte[count];
        Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static byte[] vector(String name, String key) throws Exception {
        final var values = new Properties();
        try (var input = TargetMaterializedAdmissionTest.class.getResourceAsStream(
                "/ndip3/" + name + "-vectors.properties")) {
            values.load(input);
        }
        return HexFormat.of().parseHex(values.getProperty(key));
    }
}
