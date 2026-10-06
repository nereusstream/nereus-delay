package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.InMemoryControlTargetRegistrationAuthority;
import com.nereusstream.delay.ownership.InMemoryOwnerLeaseStore;
import com.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import com.nereusstream.delay.ownership.SourceApplyCoordinator;
import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.ownership.SourceReplaySuccessor;
import com.nereusstream.delay.ownership.TargetSourceApplyRuntime;
import com.nereusstream.delay.ownership.TargetWorkerHostRuntime;
import com.nereusstream.delay.ownership.TargetWorkerOwnerActivation;
import com.nereusstream.delay.ownership.TargetWorkerShardRuntime;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.ControlAuthor;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlOperationKind;
import com.nereusstream.delay.protocol.ControlOperationRequest;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.ControlRole;
import com.nereusstream.delay.protocol.ControlRoleSet;
import com.nereusstream.delay.protocol.ControlTargetKind;
import com.nereusstream.delay.protocol.ControlTargetRef;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.ProfileBindingControlState;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.PulsarMetadata;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetControlScope;
import com.nereusstream.delay.protocol.TargetDispatchCompatibility;
import com.nereusstream.delay.protocol.TargetExpireGenerationBody;
import com.nereusstream.delay.protocol.TargetMembershipControlBody;
import com.nereusstream.delay.protocol.TargetMembershipControlRequest;
import com.nereusstream.delay.protocol.TargetMembershipGrant;
import com.nereusstream.delay.protocol.TargetMembershipPolicy;
import com.nereusstream.delay.protocol.TargetPartitionHashInput;
import com.nereusstream.delay.protocol.TargetPartitionPolicy;
import com.nereusstream.delay.protocol.TargetPublishAdmissionBody;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.protocol.TargetQuotaGrant;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlBody;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.runtime.CommandResult;
import com.nereusstream.delay.runtime.ProfileCatalog;
import com.nereusstream.delay.runtime.TargetCloseStore;
import com.nereusstream.delay.runtime.TargetCommandStore;
import com.nereusstream.delay.runtime.TargetExpireGenerationVerifier;
import com.nereusstream.delay.runtime.TargetMembershipControlStore;
import com.nereusstream.delay.runtime.TargetMembershipControlVerifier;
import com.nereusstream.delay.runtime.AttemptLedgerState;
import com.nereusstream.delay.runtime.AttemptObligationRef;
import com.nereusstream.delay.runtime.TargetClaimRecord;
import com.nereusstream.delay.runtime.TargetClaimStore;
import com.nereusstream.delay.runtime.TargetPublishAdmissionVerifier;
import com.nereusstream.delay.runtime.TargetQuotaDelta;
import com.nereusstream.delay.runtime.TargetResultRecord;
import com.nereusstream.delay.runtime.TargetMessageRecord;
import com.nereusstream.delay.runtime.TargetQuotaGrantControlVerifier;
import com.nereusstream.delay.runtime.TargetQuotaGrantStore;
import com.nereusstream.delay.runtime.TargetStoreBootstrap;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.GuardedProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** Real Kafka Worker expiry discovery and Source Apply over a test-authorized scheduled Target Message. */
final class KafkaClientArtifactTargetScheduledExpirySmoke {
    private static final byte[] LINEAGE = KafkaClientArtifactTargetWorkerSourceSmoke.bytes(16, 0x42);

    private KafkaClientArtifactTargetScheduledExpirySmoke() {}

    static void run(
            final Admin admin,
            final String bootstrap,
            final String topic,
            final String clusterId,
            final Uuid topicId,
            final KafkaClientArtifactTargetWorkerSourceSmoke.AckMode ackMode,
            final Path ackHoldFile,
            final Path ackReleaseFile,
            final Path droppedResponseFile,
            final boolean targetAdmissionScenario)
            throws Exception {
        final ShardId shard = new ShardId(com.nereusstream.delay.protocol.RouteIncarnation.random(), 0);
        final var scope = new TargetQuotaScope(
                shard, Bytes.sha256(Bytes.utf8("target-scheduled-expiry-tenant")), null);
        final String groupId = "nereus-delay-target-expiry-" + UUID.randomUUID();
        final boolean expiryNetworkAckLoss =
                ackMode == KafkaClientArtifactTargetWorkerSourceSmoke.AckMode.EXPIRY_NETWORK_RESPONSE_LOSS;
        if (expiryNetworkAckLoss != (ackHoldFile != null && ackReleaseFile != null && droppedResponseFile != null)) {
            throw new IllegalArgumentException("scheduled Target expiry ACK gate paths do not match the ACK mode");
        }
        final long signingTime = System.currentTimeMillis();
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final var actor = new ControlAuthorizationContext(
                Bytes.sha256(Bytes.utf8("target-scheduled-expiry-actor")),
                ControlRoleSet.of(ControlRole.PLATFORM_OPERATOR, ControlRole.TENANT_POLICY_ADMINISTRATOR),
                Bytes.sha256(Bytes.utf8("target-scheduled-expiry-resource")));
        final var registrations = new InMemoryControlTargetRegistrationAuthority();
        final var rootControl = KafkaClientArtifactTargetWorkerSourceSmoke.firstGrant(
                scope, shard, signingTime, actor, keys);
        registrations.register(rootControl.prepared());
        final var rootGrant = TargetQuotaGrantControlBody.decode(rootControl.mutation().canonicalBody())
                .request();

        final var rootMetadata = KafkaClientArtifactTargetWorkerSourceSmoke.produce(
                bootstrap,
                topic,
                shard.partition(),
                signingTime,
                rootControl.mutation().systemMutationId(),
                rootControl.mutation().encodeFrame());
        final KafkaSourcePosition rootPosition = position(rootMetadata, shard, clusterId, topicId);
        requireOffset(rootMetadata.offset(), 0, "root grant");

        final CanonicalTargetPartition physical = CanonicalTargetPartition.decode(
                vector("ndip3/target-compatibility-vectors.properties", "pulsar.target"));
        final var targetScope = scope.forTarget(physical.id());
        final long[] targetAmounts = rootGrant.next().limit().resources().amounts();
        Arrays.fill(targetAmounts, 50, 55, 0);
        targetAmounts[CapacityDimension.ACTIVE_MESSAGES.wireValue() - 1] = 1;
        targetAmounts[CapacityDimension.RESERVATION_MESSAGES.wireValue() - 1] = 1;
        final var targetRequest = new TargetQuotaGrantControlRequest(
                new TargetQuotaGrant(
                        targetScope,
                        Bytes.sha256(Bytes.utf8("target-scheduled-expiry-grant")),
                        1,
                        rootGrant.next().accounting(),
                        new TargetQuotaUsage(new CapacityVector(targetAmounts), 1, 64, 64, 64),
                        rootGrant.next().tenantPolicyVersion(),
                        rootGrant.next().tenantPolicyHash()),
                null,
                null);
        final var targetControl = signGrant(targetRequest, shard, actor, keys, signingTime + 600_000);
        registrations.register(targetControl.prepared());
        final var targetGrantMetadata = KafkaClientArtifactTargetWorkerSourceSmoke.produce(
                bootstrap,
                topic,
                shard.partition(),
                signingTime + 1,
                targetControl.mutation().systemMutationId(),
                targetControl.mutation().encodeFrame());
        final KafkaSourcePosition targetGrantPosition = position(targetGrantMetadata, shard, clusterId, topicId);
        requireOffset(targetGrantMetadata.offset(), 1, "Target grant");

        final var dispatch = TargetDispatchCompatibility.decode(
                vector("ndip3/target-compatibility-vectors.properties", "pulsar.journal.dispatch"));
        final var controls = new TargetControlScope(physical.id(), shard, List.of(), List.of());
        final var capability = new ProfileSemanticEnvelope(
                ProfileKind.DELIVERY_CAPABILITY, Bytes.utf8("cap"), 1, dispatch.capability());
        final var destination = new ProfileSemanticEnvelope(
                ProfileKind.DESTINATION,
                Bytes.utf8("member"),
                1,
                new DestinationProfileSemantic(
                        AdapterKind.PULSAR,
                        physical.resource(),
                        8,
                        TargetPartitionPolicy.EXPLICIT_ONLY,
                        TargetPartitionHashInput.DELAY_MESSAGE_ID,
                        List.of(5),
                        capability.ref(),
                        3,
                        60_000,
                        KafkaClientArtifactTargetWorkerSourceSmoke.bytes(32, 0xaa),
                        20_000,
                        10_000,
                        10_000,
                        1,
                        Bytes.utf8("member"),
                        86_400_000,
                        172_800_000,
                        2,
                        KafkaClientArtifactTargetWorkerSourceSmoke.bytes(32, 0xbb)));
        if (!dispatch.equals(TargetDispatchCompatibility.fromProfiles(physical, destination, capability))) {
            throw new IllegalStateException("Target expiry smoke Profiles differ from the locked dispatch vector");
        }
        final var membershipPolicy = new TargetMembershipPolicy(
                scope.tenantScope(),
                destination.ref(),
                dispatch.digest(),
                dispatch,
                controls,
                actor.tenantResourceScopeHash());
        final byte[] membershipOperation = Bytes.sha256(Bytes.utf8("target-scheduled-expiry-membership-operation"));
        final byte[] registration = TargetMembershipGrant.prepareRegistration(
                scope.tenantScope(),
                destination.ref(),
                dispatch,
                dispatch,
                controls,
                membershipPolicy.digest(),
                membershipOperation);
        final var membershipRequest = TargetMembershipControlRequest.issue(membershipPolicy, registration);
        final var membershipControl = signMembership(membershipRequest, shard, actor, keys, membershipOperation);
        registrations.register(membershipControl.prepared());
        final var membershipMetadata = KafkaClientArtifactTargetWorkerSourceSmoke.produce(
                bootstrap,
                topic,
                shard.partition(),
                signingTime + 2,
                membershipControl.mutation().systemMutationId(),
                membershipControl.mutation().encodeFrame());
        final KafkaSourcePosition membershipPosition = position(membershipMetadata, shard, clusterId, topicId);
        requireOffset(membershipMetadata.offset(), 2, "Target membership");

        final var commandIdentity = com.nereusstream.delay.protocol.SelfRoutingId.random(shard);
        final var messageIdentity = com.nereusstream.delay.protocol.SelfRoutingId.random(shard);
        final long scheduleBuildTime = commandIdentity.logicalTimestampEpochMs();
        final long expireAt = Math.addExact(scheduleBuildTime, targetAdmissionScenario ? 600_000 : 2_000);
        final long retryUntil = Math.addExact(scheduleBuildTime, 600_000);
        final var retryPolicy = new RetryPolicyRef(
                Bytes.utf8("target-scheduled-expiry-retry"),
                1,
                Bytes.sha256(Bytes.utf8("target-scheduled-expiry-retry-semantic")));
        final var intent = CanonicalScheduleIntent.create(
                destination.ref(),
                retryPolicy,
                scheduleBuildTime + 100,
                expireAt,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                Bytes.utf8("scheduled-expiry-payload"),
                null,
                AdapterMetadata.pulsar(new PulsarMetadata(null, null, null, List.of())),
                null,
                null,
                NativeDeliveryPolicy.FORBID);
        final PreparedCommand schedule = PreparedCommand.schedule(
                shard, messageIdentity.logicalId(), commandIdentity.logicalId(), intent, retryUntil);
        final var scheduleMetadata = KafkaClientArtifactTargetWorkerSourceSmoke.produce(
                bootstrap,
                topic,
                shard.partition(),
                scheduleBuildTime,
                schedule.commandId().bytes(),
                com.nereusstream.delay.protocol.CommandCodec.encodeFrame(schedule));
        final KafkaSourcePosition schedulePosition = position(scheduleMetadata, shard, clusterId, topicId);
        requireOffset(scheduleMetadata.offset(), 3, "Schedule");

        final long barrierOffset = 4;
        final var assignment = new com.nereusstream.delay.ownership.SourceAssignment(
                shard,
                Bytes.sha256(Bytes.utf8("target-scheduled-expiry-assignment-" + UUID.randomUUID())),
                1,
                new com.nereusstream.delay.protocol.KafkaActivationBarrier(
                        shard, clusterId, toUuid(topicId), barrierOffset));
        final Path storeRoot = Files.createTempDirectory("nereus-delay-kafka-target-scheduled-expiry-");
        final var leases = new OxiaOwnerLeaseStore(new InMemoryOwnerLeaseStore());
        final byte[] ownerSession = Bytes.sha256(Bytes.utf8("target-scheduled-expiry-owner-session"));
        final var acquiring = leases.acquire(
                        assignment,
                        "target-scheduled-expiry-owner",
                        ownerSession,
                        System.currentTimeMillis(),
                        60_000)
                .orElseThrow(() -> new IllegalStateException("Target expiry test Owner acquisition failed"));
        final var config = ShardStoreConfig.defaults(storeRoot);
        try {
            try (var resources = KafkaSmokeWorkerResources.open(config);
                    var store = ShardStore.openTarget(config, shard, resources)) {
                final var rootAuthority = grantAuthority(
                        registrations, keys, actor, rootPosition, rootGrant, null);
                final var initialized = TargetStoreBootstrap.commit(
                        TargetStoreBootstrap.prepare(
                                store,
                                scope,
                                LINEAGE,
                                new TargetStoreBackend.WriteLimits(128, 4 << 20),
                                KafkaClientArtifactTargetWorkerSourceSmoke.budget(),
                                rootControl.prepared(),
                                rootControl.mutation(),
                                rootPosition,
                                rootAuthority,
                                (metadata, root, complete) -> {
                                    final var stamp = complete.quota().counters().mutation();
                                    if (metadata.storeFormatVersion() != 2
                                            || !shard.equals(root.identity().shard())
                                            || stamp.sequence() != 1
                                            || !Arrays.equals(stamp.source().canonicalBytes(),
                                                    rootPosition.canonicalBytes())) {
                                        throw new IllegalStateException(
                                                "Target expiry smoke received another root bootstrap batch");
                                    }
                                }),
                        KafkaClientArtifactTargetWorkerSourceSmoke.ownerCommitAuthority(leases, acquiring));
                final var backend = initialized.backend();
                final var targetGrantStore = new TargetQuotaGrantStore(backend, scope, LINEAGE, 16, 1);
                final var grantResult = targetGrantStore.commit(
                        targetGrantStore.prepareFirst(
                                KafkaClientArtifactTargetWorkerSourceSmoke.budget(),
                                targetControl.prepared(),
                                targetControl.mutation(),
                                targetGrantPosition,
                                grantAuthority(
                                        registrations, keys, actor, targetGrantPosition, targetRequest, physical)),
                        KafkaClientArtifactTargetWorkerSourceSmoke.ownerCommitAuthority(leases, acquiring));
                if (grantResult.stableCode() != StableCode.OK) {
                    throw new IllegalStateException("Target expiry test allocation was not applied: " + grantResult);
                }
                final var membershipAuthority = membershipAuthority(
                        registrations, actor, keys, membershipPolicy, destination, capability);
                final var membershipStore = new TargetMembershipControlStore(backend, scope, LINEAGE, 16, 1);
                final var membershipResult = membershipStore.commit(
                        membershipStore.prepareFirst(
                                KafkaClientArtifactTargetWorkerSourceSmoke.budget(),
                                membershipControl.prepared(),
                                membershipControl.mutation(),
                                membershipPosition,
                                physical,
                                membershipAuthority),
                        KafkaClientArtifactTargetWorkerSourceSmoke.ownerCommitAuthority(leases, acquiring));
                if (membershipResult.stableCode() != StableCode.OK) {
                    throw new IllegalStateException(
                            "Target expiry test membership was not applied: " + membershipResult);
                }
                final var membership = TargetMembershipGrant.fromRegistration(
                        registration, membershipControl.mutation().mutationHash(), membershipPosition);
                final byte[] activationKey = Bytes.concat(
                        new byte[] {TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG, TargetKeyCodec.KEY_FORMAT},
                        targetScope.keySuffix());
                final var activation = TargetQuotaGrantActivation.decode(TargetValueEnvelope.decode(
                                store.get(ColumnFamily.META, activationKey), TargetQuotaGrantActivation.VALUE_TYPE)
                        .payload());
                final var profiles = ProfileBindingControlState.empty()
                        .activate(destination.ref(), rootPosition)
                        .activate(capability.ref(), membershipPosition);
                final var commands = new TargetCommandStore(backend, scope, LINEAGE, 16, 1);
                final var commandPolicy = new TargetCommandStore.Policy(
                        scope,
                        600_000,
                        600_000,
                        10_000,
                        Set.of(ProtocolTuple.managedCommand()),
                        new TargetCommandStore.DeliveryWindow(60_000, 1, 600_000));
                final TargetCommandStore.Schedules schedules = (incoming, source) -> {
                    final var proposed = new TargetScheduleBinding(
                            incoming.delayMessageId(),
                            incoming.type(),
                            incoming.canonicalBody(),
                            source,
                            physical.id(),
                            new TargetKeyCodec.Domain(0, 1),
                            activation.allocation().identity().accountingIncarnation(),
                            membership.required().digest(),
                            membership.offered().digest(),
                            membership.controls().digest(),
                            membership.digest(),
                            null,
                            null);
                    return new TargetCommandStore.ScheduleAdmission(
                            StableCode.OK,
                            new com.nereusstream.delay.runtime.TargetScheduleRegistration.Authority(
                                    proposed, physical, destination, capability, profiles, 0),
                            com.nereusstream.delay.runtime.TargetOrderState.OrderingContract.ADMISSION_WATERMARK);
                };
                final CommandResult scheduleResult = commands.commit(
                        commands.prepareFirst(
                                KafkaClientArtifactTargetWorkerSourceSmoke.budget(),
                                schedule,
                                schedulePosition,
                                commandPolicy,
                                (reader, binding, source) -> false,
                                (reader, binding) -> Optional.empty(),
                                schedules,
                                (binding, source) -> {
                                    throw new AssertionError("inline Target expiry payload resolved proof authority");
                                }),
                        KafkaClientArtifactTargetWorkerSourceSmoke.ownerCommitAuthority(leases, acquiring));
                if (scheduleResult.stableCode() != StableCode.SCHEDULED
                        || store.shardMutationSequence() != 4
                        || !schedulePosition.equals(store.appliedShardLogPosition())) {
                    throw new IllegalStateException("Target expiry smoke did not persist the exact Schedule source: "
                            + scheduleResult.stableCode() + ", mutationSequence=" + store.shardMutationSequence()
                            + ", appliedPosition=" + store.appliedShardLogPosition());
                }
                final var scheduledMessage = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                                store.get(ColumnFamily.ID, TargetKeyCodec.message(schedule.delayMessageId())),
                                TargetMessageRecord.VALUE_TYPE)
                        .payload());
                if (scheduledMessage.expireAtEpochMs() != intent.expireAtEpochMs()
                        || scheduledMessage.runtime().terminal()) {
                    throw new IllegalStateException("Target expiry smoke did not persist a live expiring Message");
                }

                final var active = TargetWorkerOwnerActivation.activate(
                        initialized, store, assignment, acquiring, leases, System::currentTimeMillis);
                final TargetAdmissionFixture targetAdmission = targetAdmissionScenario
                        ? appendTargetAdmission(
                                bootstrap,
                                topic,
                                clusterId,
                                topicId,
                                store,
                                backend,
                                scope,
                                activation,
                                intent,
                                scheduledMessage,
                                physical,
                                active,
                                leases,
                                keys)
                        : null;
                final var expiryProof = new AtomicReference<TrustedUtcIntervalEvidence>();
                final var expiryAuthorityResolutions = new AtomicInteger();
                final var admissionAuthorityResolutions = new AtomicInteger();
                final var closeControls = new TargetCloseStore(backend, scope, LINEAGE, 16, 1)
                        .reservationControls((reader, binding) -> Optional.empty());
                final var sourceRuntime = new TargetSourceApplyRuntime(
                        initialized,
                        store,
                        assignment,
                        active,
                        new TargetSourceApplyRuntime.Authorities(
                                leases,
                                SourceReplaySuccessor.strictKafka(),
                                entry -> { throw new AssertionError("expiry did not resolve a grant"); },
                                entry -> { throw new AssertionError("expiry did not resolve a time fence"); },
                                entry -> expiryControl(
                                        entry,
                                        scope,
                                        schedule.delayMessageId(),
                                        scheduledMessage.expireAtEpochMs(),
                                        clusterId,
                                        topicId,
                                        active,
                                        keys,
                                        leases,
                                        expiryProof,
                                        expiryAuthorityResolutions),
                                entry -> { throw new AssertionError("expiry did not resolve Target Close"); },
                                        entry -> { throw new AssertionError("expiry did not resolve membership"); },
                                        KafkaClientArtifactTargetWorkerSourceSmoke.ownerCommitAuthority(leases, active),
                                        KafkaClientArtifactTargetWorkerSourceSmoke.ownerReadAuthority(leases, active),
                                        entry -> { throw new AssertionError("expiry source was a Client Command"); },
                                        entry -> {
                                            throw new AssertionError("unexpected Target Native policy control");
                                        },
                                        entry -> {
                                            if (targetAdmission == null
                                                    || !sameAdmissionEntry(targetAdmission.sourceEntry(), entry)) {
                                                throw new IllegalStateException(
                                                        "Target Admission source differs from the persisted fixture");
                                            }
                                            final var expectedAuthor = AuthorIdentity.owner(
                                                    Bytes.utf8("kafka-target-worker-deployment"),
                                                    Bytes.utf8("kafka-target-worker-host-run"),
                                                    active.ownerEpoch(),
                                                    Bytes.sha256(active.leaseToken()));
                                            return new TargetSourceApplyRuntime.AdmissionControl(
                                                    (actualScope, writer, mutation, source) -> {
                                                        if (!scope.equals(actualScope)
                                                        || !Arrays.equals(
                                                                        expectedAuthor.canonicalBytes(),
                                                                        writer.canonicalBytes())
                                                                || !Arrays.equals(
                                                                        targetAdmission.mutation().canonicalEnvelope(),
                                                                        mutation.canonicalEnvelope())
                                                                || !Arrays.equals(
                                                                        entry.position().canonicalBytes(),
                                                                        source.canonicalBytes())) {
                                                            throw new IllegalStateException(
                                                                    "Target Admission verifier received another Owner or source");
                                                        }
                                                        admissionAuthorityResolutions.incrementAndGet();
                                                        return new TargetPublishAdmissionVerifier.Authorization(
                                                                keys.getPublic(),
                                                                ProtocolTuple.targetPublishAdmission(),
                                                                20_000,
                                                                10_000,
                                                                60_000,
                                                                (boundScope, boundWriter, boundSource, evidence) -> true);
                                                    },
                                                    KafkaClientArtifactTargetWorkerSourceSmoke.ownerCommitAuthority(
                                                            leases, active));
                                        }),
                        new TargetSourceApplyRuntime.Limits(2048, 16L << 20, 60_000_000_000L, 16, 1),
                        System::nanoTime);
                final var consumer = KafkaClientArtifactTargetWorkerSourceSmoke.newSourceConsumer(
                        bootstrap,
                        groupId,
                        clusterId,
                        topic,
                        toUuid(topicId),
                        shard,
                        ackMode);
                final var producerConfig = Map.<String, Object>of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        bootstrap,
                        ProducerConfig.ACKS_CONFIG,
                        "all",
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        ByteArraySerializer.class.getName(),
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        ByteArraySerializer.class.getName());
                TargetWorkerHostRuntime host = null;
                Throwable primaryFailure = null;
                final CountDownLatch expiryRequest = new CountDownLatch(1);
                final CountDownLatch expiryAppendFinished = new CountDownLatch(1);
                final AtomicReference<com.nereusstream.delay.ownership.ShardLogMutationAppender.AppendOutcome>
                        expiryAppendOutcome = new AtomicReference<>();
                final AtomicReference<Throwable> expiryAppendFailure = new AtomicReference<>();
                try (var producer = new KafkaProducer<byte[], byte[]>(producerConfig);
                        var appender = new KafkaClientArtifactShardLogMutationAppender(
                                (GuardedProducer<byte[], byte[]>) producer,
                                shard,
                                clusterId,
                                topic,
                                toUuid(topicId),
                                Duration.ofSeconds(15))) {
                    final var workerClasses = KafkaClientArtifactTargetWorkerSourceSmoke.workClasses();
                    final var maintenance = new TargetWorkerShardRuntime.Maintenance(
                            closeControls,
                            new com.nereusstream.delay.ownership.TargetReservationClosureWorkClassExecutor.Limits(
                                    4096, 250_000, 60_000_000_000L),
                            new com.nereusstream.delay.ownership.TargetReservationExpiryWorkClassExecutor.Limits(
                                    2048, 100_000, 60_000_000_000L),
                            KafkaClientArtifactTargetWorkerSourceSmoke.ownerCommitAuthority(leases, active),
                            delta -> { throw new AssertionError("unexpected Close materialization"); },
                            delta -> { throw new AssertionError("unexpected reservation expiry"); },
                            delta -> { throw new AssertionError("unexpected Close cursor update"); },
                            System::currentTimeMillis);
                    final var worker = KafkaClientArtifactTargetWorkerSourceFactory.create(
                            consumer,
                            topic,
                            Duration.ofSeconds(5),
                            assignment,
                            workerClasses,
                            store,
                            resources,
                            sourceRuntime,
                            maintenance);
                    final com.nereusstream.delay.ownership.ShardLogMutationAppender observedAppender = mutation -> {
                        try {
                            final var outcome = appender.append(mutation);
                            expiryAppendOutcome.set(outcome);
                            return outcome;
                        } catch (RuntimeException | Error failure) {
                            expiryAppendFailure.set(failure);
                            throw failure;
                        } finally {
                            expiryAppendFinished.countDown();
                        }
                    };
                    if (!targetAdmissionScenario) {
                        worker.configureMessageExpiryMaintenance(
                                observedAppender,
                                () -> {
                                    final long now = System.currentTimeMillis();
                                    final var evidence = expiryEvidence(now);
                                    expiryProof.set(evidence);
                                    expiryRequest.countDown();
                                    return new TargetWorkerShardRuntime.MessageExpiryRequest(
                                            KafkaClientArtifactTargetWorkerSourceSmoke.budget(),
                                            evidence,
                                            Math.addExact(now, 600_000),
                                            new OwnerIdentity(
                                                    Bytes.utf8("kafka-target-worker-deployment"),
                                                    Bytes.utf8("kafka-target-worker-host-run"),
                                                    active.ownerEpoch(),
                                                    Bytes.sha256(active.leaseToken())),
                                            1,
                                            keys.getPrivate());
                                });
                        final long remaining =
                                scheduledMessage.expireAtEpochMs() - System.currentTimeMillis();
                        if (remaining > 0) {
                            TimeUnit.MILLISECONDS.sleep(remaining);
                        }
                        if (expiryNetworkAckLoss) {
                            final var candidate = worker.discoverMessageExpiry(
                                            KafkaClientArtifactTargetWorkerSourceSmoke.budget(),
                                            null,
                                            expiryEvidence(System.currentTimeMillis()),
                                            System::currentTimeMillis)
                                    .candidate()
                                    .orElseThrow(() -> new IllegalStateException(
                                            "scheduled Target Message was not discoverable before Host startup"));
                            if (!schedule.delayMessageId().equals(candidate.locator().messageId())
                                    || candidate.expireAtEpochMs() != scheduledMessage.expireAtEpochMs()) {
                                throw new IllegalStateException(
                                        "scheduled Target expiry discovery selected another Message generation");
                            }
                        }
                    } else {
                        worker.configureMessageExpiryMaintenance(
                                mutation -> {
                                    throw new AssertionError(
                                            "Target Admission ACK smoke unexpectedly appended Message expiry: "
                                                    + mutation.type());
                                },
                                () -> {
                                    final long now = System.currentTimeMillis();
                                    final var evidence = expiryEvidence(now);
                                    return new TargetWorkerShardRuntime.MessageExpiryRequest(
                                            KafkaClientArtifactTargetWorkerSourceSmoke.budget(),
                                            evidence,
                                            Math.addExact(now, 600_000),
                                            new OwnerIdentity(
                                                    Bytes.utf8("kafka-target-worker-deployment"),
                                                    Bytes.utf8("kafka-target-worker-host-run"),
                                                    active.ownerEpoch(),
                                                    Bytes.sha256(active.leaseToken())),
                                            1,
                                            keys.getPrivate());
                                });
                        Files.writeString(ackHoldFile, "hold\n");
                    }
                    host = TargetWorkerHostRuntime.start(
                            workerClasses,
                            resources,
                            List.of(worker),
                            new SchedulerBudget(64, 4L << 20, TimeUnit.SECONDS.toNanos(10)),
                            Duration.ofMillis(50),
                            failure -> {});
                    if (targetAdmissionScenario) {
                        runTargetAdmissionAckLoss(
                                admin,
                                topic,
                                groupId,
                                host,
                                worker,
                                store,
                                scope,
                                targetAdmission,
                                ackReleaseFile,
                                droppedResponseFile,
                                admissionAuthorityResolutions);
                    } else {
                    if (!expiryRequest.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Target Host did not request expiry discovery");
                    }
                    // Source polling holds the selected Worker turn lock; let its maintenance append finish first.
                    if (!expiryAppendFinished.await(20, TimeUnit.SECONDS)) {
                        throw new IllegalStateException(
                                "Target Host did not append the scheduled expiry through its maintenance work class",
                                expiryAppendFailure.get());
                    }
                    final var expiryAppend = expiryAppendOutcome.get();
                    if (expiryAppend == null
                            || expiryAppend.disposition()
                                    != com.nereusstream.delay.ownership.ShardLogMutationAppender.AppendDisposition
                                            .PERSISTED
                            || !(expiryAppend.sourcePosition() instanceof KafkaSourcePosition appendedPosition)
                            || appendedPosition.offset() != barrierOffset
                            || !rootPosition.sameSourceIdentity(appendedPosition)) {
                        throw new IllegalStateException(
                                "Target Host did not persist the scheduled expiry at the exact next source offset: "
                                        + expiryAppend,
                                expiryAppendFailure.get());
                    }
                    final SourceApplyCoordinator.TurnResult turn;
                    if (expiryNetworkAckLoss) {
                        Files.writeString(ackHoldFile, "hold\n");
                        final var ackUnknown =
                                KafkaClientArtifactTargetWorkerSourceSmoke.runUntilAckUnknownByHost(host);
                        if (ackUnknown.status() != SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN
                                || !(ackUnknown.entry() instanceof SourceReplayMutation unknownExpiry)
                                || unknownExpiry.mutation().type() != SystemMutationType.EXPIRE_GENERATION
                                || !(unknownExpiry.position() instanceof KafkaSourcePosition unknownPosition)
                                || unknownPosition.offset() != barrierOffset
                                || ackUnknown.appliedOutcome() != null) {
                            throw new IllegalStateException(
                                    "scheduled Target expiry did not retain its exact entry at ACK_UNKNOWN: "
                                            + ackUnknown.status() + ", entry=" + ackUnknown.entry());
                        }
                        final String dropped = Files.exists(droppedResponseFile)
                                ? Files.readString(droppedResponseFile)
                                : "";
                        if (!dropped.contains("apiKey=8")
                                || !dropped.contains("brokerResponseReceived=true forwarded=false")) {
                            throw new IllegalStateException(
                                    "TCP proxy did not withhold the scheduled expiry Broker ACK response: "
                                            + dropped);
                        }
                        final var appliedUnknown = store.appliedShardLogPosition();
                        if (!(appliedUnknown instanceof KafkaSourcePosition unknownApplied)
                                || unknownApplied.offset() != barrierOffset
                                || !rootPosition.sameSourceIdentity(unknownApplied)) {
                            throw new IllegalStateException(
                                    "scheduled expiry was not durably applied before TCP ACK response loss");
                        }
                        final var committedUnknown = admin.listConsumerGroupOffsets(groupId)
                                .partitionsToOffsetAndMetadata()
                                .get(10, TimeUnit.SECONDS)
                                .get(new TopicPartition(topic, shard.partition()));
                        if (committedUnknown == null || committedUnknown.offset() != barrierOffset + 1) {
                            throw new IllegalStateException(
                                    "Broker did not commit the scheduled expiry before its ACK response was lost");
                        }
                        final long sequenceAfterUnknown = store.latestSequenceNumber();
                        final long mutationsAfterUnknown = store.shardMutationSequence();
                        Files.writeString(ackReleaseFile, "release\n");
                        turn = KafkaClientArtifactTargetWorkerSourceSmoke.runUntilAppliedByHost(host);
                        if (turn.status() != SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED
                                || !unknownExpiry.equals(turn.entry())
                                || turn.appliedOutcome() == null
                                || turn.appliedOutcome().systemMutationResult().stableCode() != StableCode.OK
                                || store.latestSequenceNumber() != sequenceAfterUnknown
                                || store.shardMutationSequence() != mutationsAfterUnknown
                                || expiryAuthorityResolutions.get() != 1) {
                            throw new IllegalStateException(
                                    "scheduled expiry ACK retry changed Store state or re-resolved authority");
                        }
                        System.out.println(
                                "Kafka Target scheduled EXPIRE_GENERATION TCP ACK-loss recovery passed: Broker "
                                        + "committed offset " + barrierOffset
                                        + " before response loss; same Host retried only ACK without another "
                                        + "Store mutation.");
                    } else {
                        turn = KafkaClientArtifactTargetWorkerSourceSmoke.runUntilAppliedByHost(host);
                    }
                    if (turn.status() != SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED
                            || !(turn.entry() instanceof SourceReplayMutation expiryEntry)
                            || expiryEntry.mutation().type() != SystemMutationType.EXPIRE_GENERATION
                            || !(expiryEntry.position() instanceof KafkaSourcePosition expiryPosition)
                            || expiryPosition.offset() != barrierOffset
                            || turn.appliedOutcome() == null
                            || turn.appliedOutcome().systemMutationResult().applyStatus() != ApplyStatus.APPLIED
                            || turn.appliedOutcome().systemMutationResult().stableCode() != StableCode.OK) {
                        throw new IllegalStateException("Target Host did not apply and ACK scheduled Message expiry: "
                                + turn.status() + ", outcome=" + turn.appliedOutcome());
                    }
                    final var expiredBody = TargetExpireGenerationBody.decode(expiryEntry.mutation().canonicalBody());
                    if (!schedule.delayMessageId().equals(expiredBody.messageId())
                            || expiredBody.expireAt() != scheduledMessage.expireAtEpochMs()) {
                        throw new IllegalStateException(
                                "Target expiry appender selected a different Message generation");
                    }
                    final var expired = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.ID, TargetKeyCodec.message(schedule.delayMessageId())),
                                    TargetMessageRecord.VALUE_TYPE)
                            .payload());
                    if (expired.runtime().aggregateState()
                                    != com.nereusstream.delay.runtime.GenerationAggregateState.EXPIRED
                            || !expired.runtime().terminal()
                            || store.get(
                                            ColumnFamily.TIMELINE,
                                            new com.nereusstream.delay.runtime.TargetExpiryRef(
                                                            expired.locator(), expired.expireAtEpochMs())
                                                    .encodedKey())
                                    != null
                            || store.shardMutationSequence() != 5) {
                        throw new IllegalStateException("Target expiry did not atomically terminalize the Message");
                    }
                    final var applied = store.appliedShardLogPosition();
                    if (!(applied instanceof KafkaSourcePosition appliedExpiry)
                            || appliedExpiry.offset() != barrierOffset
                            || !rootPosition.sameSourceIdentity(appliedExpiry)) {
                        throw new IllegalStateException("Target Store did not advance through the expiry source");
                    }
                    final var committedOffsets = admin.listConsumerGroupOffsets(groupId)
                            .partitionsToOffsetAndMetadata()
                            .get(10, TimeUnit.SECONDS);
                    final var committed = committedOffsets.get(new TopicPartition(topic, shard.partition()));
                    if (committed == null || committed.offset() != barrierOffset + 1) {
                        throw new IllegalStateException("Kafka Broker did not commit the automatic expiry record");
                    }
                    System.out.println("Kafka Target automatic scheduled-Message expiry passed: Schedule was"
                            + " persisted at offset 3, Host discovered Message " + schedule.delayMessageId()
                            + ", and generated EXPIRE_GENERATION offset " + barrierOffset
                            + " terminalized it with Store/group frontiers " + appliedExpiry.offset() + "/"
                            + committed.offset() + ". Grant, membership, Owner, signing and time authorities"
                            + " are explicit test fixtures. expiryAuthorityResolutions="
                            + expiryAuthorityResolutions.get() + ".");
                    }
                } catch (Exception | Error failure) {
                    primaryFailure = failure;
                    throw failure;
                } finally {
                    try {
                        consumer.close();
                    } catch (RuntimeException | Error cleanupFailure) {
                        if (primaryFailure == null) {
                            throw cleanupFailure;
                        }
                        primaryFailure.addSuppressed(cleanupFailure);
                    }
                    if (host != null) {
                        try {
                            final var drained = host.drainAll(
                                    new com.nereusstream.delay.ownership.TargetOwnerDrainCoordinator.Request(
                                            Math.addExact(System.currentTimeMillis(), 60_000),
                                            new SchedulerBudget(64, 4L << 20, TimeUnit.SECONDS.toNanos(10))),
                                    new SchedulerBudget(64, 4L << 20, TimeUnit.SECONDS.toNanos(10)),
                                    System::currentTimeMillis);
                            if (!drained.complete()
                                    || drained.shards().size() != 1
                                    || drained.shards().getFirst().status()
                                            != TargetWorkerHostRuntime.Status.RELEASED
                                    || host.firstMaintenanceFailure() != null) {
                                throw new IllegalStateException(
                                        "Target scheduled-expiry Host did not drain cleanly: " + drained.shards(),
                                        host.firstMaintenanceFailure());
                            }
                        } catch (RuntimeException | Error cleanupFailure) {
                            if (primaryFailure == null) {
                                throw cleanupFailure;
                            }
                            primaryFailure.addSuppressed(cleanupFailure);
                        }
                    } else {
                        leases.release(active);
                    }
                }
            }
        } finally {
            leases.current(shard).ifPresent(leases::release);
            KafkaClientArtifactTargetWorkerSourceSmoke.deleteTree(storeRoot);
        }
    }

    private static boolean sameAdmissionEntry(final SourceReplayMutation expected, final Object candidate) {
        if (!(candidate instanceof SourceReplayMutation actual)) {
            return false;
        }
        final KafkaSourcePosition expectedPosition = (KafkaSourcePosition) expected.position();
        final KafkaSourcePosition actualPosition = (KafkaSourcePosition) actual.position();
        return Arrays.equals(
                        expected.mutation().canonicalEnvelope(), actual.mutation().canonicalEnvelope())
                && expectedPosition.shardId().equals(actualPosition.shardId())
                && expectedPosition.authenticatedClusterId().equals(actualPosition.authenticatedClusterId())
                && expectedPosition.nativeTopicUuid().equals(actualPosition.nativeTopicUuid())
                && expectedPosition.offset() == actualPosition.offset()
                // Producer RecordMetadata has no leader epoch; the consumed Broker record does.
                && (expectedPosition.leaderEpoch() == null
                        || expectedPosition.leaderEpoch().equals(actualPosition.leaderEpoch()))
                && expectedPosition.brokerLogAppendTimeEpochMs() == actualPosition.brokerLogAppendTimeEpochMs()
                && java.util.Objects.equals(
                        expected.sourceConnectionGeneration(), actual.sourceConnectionGeneration())
                && Arrays.equals(expected.guardAttestationDigest(), actual.guardAttestationDigest());
    }

    private static void runTargetAdmissionAckLoss(
            final Admin admin,
            final String topic,
            final String groupId,
            final TargetWorkerHostRuntime host,
            final TargetWorkerShardRuntime worker,
            final ShardStore store,
            final TargetQuotaScope scope,
            final TargetAdmissionFixture fixture,
            final Path ackReleaseFile,
            final Path droppedResponseFile,
            final AtomicInteger authorityResolutions)
            throws Exception {
        final var firstTurn = KafkaClientArtifactTargetWorkerSourceSmoke.runUntilAckUnknownByHost(host);
        if (firstTurn.status() != SourceApplyCoordinator.TurnStatus.ACK_UNKNOWN
                || !sameAdmissionEntry(fixture.sourceEntry(), firstTurn.entry())
                || firstTurn.appliedOutcome() != null
                || worker.pendingSourceEntry()
                        .filter(entry -> sameAdmissionEntry(fixture.sourceEntry(), entry))
                        .isEmpty()) {
            throw new IllegalStateException(
                    "Target Admission Broker ACK loss did not retain its exact applied source entry: "
                            + firstTurn.status() + ", entry=" + firstTurn.entry());
        }
        final String dropped = Files.exists(droppedResponseFile) ? Files.readString(droppedResponseFile) : "";
        if (!dropped.contains("apiKey=8")
                || !dropped.contains("brokerResponseReceived=true forwarded=false")) {
            throw new IllegalStateException("TCP proxy did not withhold the Target Admission Broker ACK response: "
                    + dropped);
        }
        final var appliedEntry = (SourceReplayMutation) firstTurn.entry();
        final var applied = store.appliedShardLogPosition();
        if (!appliedEntry.position().equals(applied) || store.shardMutationSequence() != 5) {
            throw new IllegalStateException("Target Admission was not durably applied before Broker ACK loss");
        }
        final var committedBeforeRetry = admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get(10, TimeUnit.SECONDS)
                .get(new TopicPartition(topic, scope.shard().partition()));
        if (committedBeforeRetry == null || committedBeforeRetry.offset() != fixture.position().offset() + 1) {
            throw new IllegalStateException("Broker did not commit Target Admission before its ACK response was lost");
        }
        assertTargetAdmissionApplied(store, fixture, appliedEntry);
        if (authorityResolutions.get() != 1) {
            throw new IllegalStateException("Target Admission authority was not resolved exactly once");
        }
        final long sequenceAfterUnknown = store.latestSequenceNumber();
        final long mutationsAfterUnknown = store.shardMutationSequence();
        Files.writeString(ackReleaseFile, "release\n");
        final var retry = KafkaClientArtifactTargetWorkerSourceSmoke.runUntilAppliedByHost(host);
        if (retry.status() != SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED
                || !sameAdmissionEntry(fixture.sourceEntry(), retry.entry())
                || retry.appliedOutcome() == null
                || retry.appliedOutcome().systemMutationResult().applyStatus() != ApplyStatus.APPLIED
                || retry.appliedOutcome().systemMutationResult().stableCode() != StableCode.OK
                || worker.pendingSourceEntry().isPresent()
                || store.latestSequenceNumber() != sequenceAfterUnknown
                || store.shardMutationSequence() != mutationsAfterUnknown
                || authorityResolutions.get() != 1) {
            throw new IllegalStateException("Target Admission retry changed Store state or re-resolved authority: "
                    + retry.status() + ", entry=" + retry.entry());
        }
        final var committedAfterRetry = admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get(10, TimeUnit.SECONDS)
                .get(new TopicPartition(topic, scope.shard().partition()));
        if (committedAfterRetry == null || committedAfterRetry.offset() != fixture.position().offset() + 1) {
            throw new IllegalStateException("Broker frontier changed after the Target Admission ACK retry");
        }
        assertTargetAdmissionApplied(store, fixture, appliedEntry);
        System.out.println("Kafka Target PUBLISH_ADMISSION TCP ACK-loss recovery passed: Broker committed offset "
                + fixture.position().offset() + " before response loss; same Host retried only ACK without another "
                + "Store mutation. Claim, AttemptBudget, first result and source frontier remain durable.");
    }

    private static void assertTargetAdmissionApplied(
            final ShardStore store,
            final TargetAdmissionFixture fixture,
            final SourceReplayMutation appliedEntry) {
        final var claim = fixture.claim();
        final var entry = appliedEntry;
        final var message = TargetMessageRecord.decode(TargetValueEnvelope.decode(
                        store.get(ColumnFamily.ID, TargetKeyCodec.message(claim.work().locator().messageId())),
                        TargetMessageRecord.VALUE_TYPE)
                .payload());
        final byte[] systemKey = Bytes.concat(
                new byte[] {(byte) TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT},
                entry.mutation().systemMutationId());
        final byte[] rawFirst = store.get(ColumnFamily.DEDUPE, systemKey);
        final String firstResult = rawFirst == null
                ? "missing"
                : com.nereusstream.delay.runtime.SystemMutationResult.decode(TargetResultRecord.decode(
                                TargetValueEnvelope.decode(rawFirst, TargetResultRecord.VALUE_TYPE).payload())
                        .typedPayload()).toString();
        final byte[] rawClaim = store.get(ColumnFamily.INFLIGHT, claim.key());
        final byte[] rawCharge = store.get(ColumnFamily.META, claim.chargeKey());
        final boolean publishing = message.runtime().currentWorkKind()
                == com.nereusstream.delay.runtime.CurrentSendWorkKind.PUBLISHING;
        final boolean attemptMatches = Arrays.equals(fixture.attemptId(), message.runtime().publishAttemptId());
        if (!publishing || !attemptMatches || rawClaim != null || rawCharge != null) {
            throw new IllegalStateException("Target Admission did not atomically consume the exact Claim: work="
                    + message.runtime().currentWorkKind() + ", attemptMatches=" + attemptMatches
                    + ", claimPresent=" + (rawClaim != null) + ", chargePresent=" + (rawCharge != null)
                    + ", result=" + firstResult);
        }
        final byte[] budgetKey = Bytes.concat(
                new byte[] {(byte) TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG, TargetKeyCodec.KEY_FORMAT},
                fixture.attemptId());
        final var budget = TargetQuotaAttemptBudget.decode(TargetValueEnvelope.decode(
                        store.get(ColumnFamily.META, budgetKey), TargetQuotaAttemptBudget.VALUE_TYPE)
                .payload());
        if (budget.phase() != TargetQuotaAttemptBudget.Phase.ADMITTED
                || !claim.work().locator().equals(budget.locator())) {
            throw new IllegalStateException("Target Admission did not persist the admitted attempt budget");
        }
        final var first = TargetResultRecord.decode(TargetValueEnvelope.decode(
                        store.get(ColumnFamily.DEDUPE, systemKey), TargetResultRecord.VALUE_TYPE)
                .payload());
        final var result = com.nereusstream.delay.runtime.SystemMutationResult.decode(first.typedPayload());
        final byte[] positionKey = Bytes.concat(
                new byte[] {(byte) TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT},
                entry.position().canonicalBytes());
        final var position = TargetResultRecord.decode(TargetValueEnvelope.decode(
                        store.get(ColumnFamily.DEDUPE, positionKey), TargetResultRecord.VALUE_TYPE)
                .payload());
        if (result.applyStatus() != ApplyStatus.APPLIED
                || result.stableCode() != StableCode.OK
                || !Arrays.equals(first.logicalId(), entry.mutation().systemMutationId())) {
            throw new IllegalStateException("Target Admission immutable first result is missing or incorrect");
        }
        position.requireFirst(first);
    }

    private static TargetSourceApplyRuntime.ExpiryControl expiryControl(
            final SourceReplayMutation entry,
            final TargetQuotaScope scope,
            final com.nereusstream.delay.protocol.DelayMessageId expectedMessage,
            final long expectedExpiry,
            final String clusterId,
            final Uuid topicId,
            final com.nereusstream.delay.ownership.OwnerLease active,
            final KeyPair keys,
            final OxiaOwnerLeaseStore leases,
            final AtomicReference<TrustedUtcIntervalEvidence> expectedEvidence,
            final AtomicInteger expiryAuthorityResolutions) {
        final var mutation = entry.mutation();
        final var body = TargetExpireGenerationBody.decode(mutation.canonicalBody());
        final var expectedAuthor = AuthorIdentity.owner(
                Bytes.utf8("kafka-target-worker-deployment"),
                Bytes.utf8("kafka-target-worker-host-run"),
                active.ownerEpoch(),
                Bytes.sha256(active.leaseToken()));
        if (!expectedMessage.equals(body.messageId())
                || body.expireAt() != expectedExpiry
                || body.generation() != 0
                || !Arrays.equals(expectedAuthor.canonicalBytes(), mutation.authorIdentity())
                || !(entry.position() instanceof KafkaSourcePosition recordPosition)
                || !clusterId.equals(recordPosition.authenticatedClusterId())
                || !toUuid(topicId).equals(recordPosition.nativeTopicUuid())) {
            throw new IllegalStateException("expiry Source Apply received another Message, Owner or Kafka source");
        }
        return new TargetSourceApplyRuntime.ExpiryControl(
                (actualScope, author, actualMutation, source) -> {
                    if (!scope.equals(actualScope)
                            || !Arrays.equals(expectedAuthor.canonicalBytes(), author.canonicalBytes())
                            || !mutation.equals(actualMutation)
                            || !clusterId.equals(((KafkaSourcePosition) source).authenticatedClusterId())
                            || !toUuid(topicId).equals(((KafkaSourcePosition) source).nativeTopicUuid())) {
                        throw new IllegalStateException("expiry verifier authority received another source record");
                    }
                    expiryAuthorityResolutions.incrementAndGet();
                    return new TargetExpireGenerationVerifier.Authorization(
                            keys.getPublic(),
                            1,
                            (proofScope, proofAuthor, proofSource, evidence) -> {
                                final var expected = expectedEvidence.get();
                                return expected != null
                                        && scope.equals(proofScope)
                                        && Arrays.equals(expectedAuthor.canonicalBytes(), proofAuthor.canonicalBytes())
                                        && clusterId.equals(((KafkaSourcePosition) proofSource)
                                                .authenticatedClusterId())
                                        && toUuid(topicId).equals(((KafkaSourcePosition) proofSource)
                                                .nativeTopicUuid())
                                        && Arrays.equals(expected.canonicalBytes(), evidence.canonicalBytes());
                            });
                },
                KafkaClientArtifactTargetWorkerSourceSmoke.ownerCommitAuthority(leases, active));
    }

    private static TargetQuotaGrantControlVerifier.Authority grantAuthority(
            final InMemoryControlTargetRegistrationAuthority registrations,
            final KeyPair keys,
            final ControlAuthorizationContext actor,
            final KafkaSourcePosition expectedSource,
            final TargetQuotaGrantControlRequest request,
            final CanonicalTargetPartition expectedAllocation) {
        return new TargetQuotaGrantControlVerifier.Authority(
                registrations,
                (version, source) -> version == 1 ? keys.getPublic() : null,
                (actualScope, source) -> {
                    actualScope.requireRoute(expectedSource.shardId(), request.next().scope().tenantScope());
                    if (!expectedSource.sameSourceIdentity(source)) {
                        throw new IllegalStateException("Target quota grant source identity changed");
                    }
                },
                (body, view, source, allocation) -> {
                    if (!request.equals(body.request())
                            || !expectedSource.equals(source)
                            || (expectedAllocation == null) != (allocation == null)
                            || (allocation != null
                                    && (!expectedAllocation.id().equals(allocation.identity().target())
                                            || !Arrays.equals(LINEAGE, allocation.recoveryLineage())))) {
                        throw new IllegalStateException("Target quota grant allocation authority changed");
                    }
                },
                actor,
                prepared -> true);
    }

    private static TargetMembershipControlVerifier.Authority membershipAuthority(
            final InMemoryControlTargetRegistrationAuthority registrations,
            final ControlAuthorizationContext actor,
            final KeyPair keys,
            final TargetMembershipPolicy policy,
            final ProfileSemanticEnvelope destination,
            final ProfileSemanticEnvelope capability) {
        final ProfileCatalog profiles = new ProfileCatalog() {
            @Override
            public ProfileSemanticEnvelope resolve(final ProfileRef reference) {
                return reference.equals(destination.ref())
                        ? destination
                        : reference.equals(capability.ref()) ? capability : null;
            }

            @Override
            public com.nereusstream.delay.protocol.CredentialBinding resolveBinding(
                    final ProfileRef reference, final long generation) {
                throw new AssertionError("Target membership expiry fixture has no credential binding");
            }

            @Override
            public com.nereusstream.delay.protocol.CredentialBindingHead resolveHead(final ProfileRef reference) {
                throw new AssertionError("Target membership expiry fixture has no credential head");
            }

            @Override
            public com.nereusstream.delay.protocol.CredentialBindingProtection resolveProtection(
                    final ProfileRef reference, final long generation) {
                throw new AssertionError("Target membership expiry fixture has no credential protection");
            }
        };
        return new TargetMembershipControlVerifier.Authority(
                registrations,
                (reference, source, operation) -> Arrays.equals(reference, policy.digest()) ? policy : null,
                (version, source) -> version == 1 ? keys.getPublic() : null,
                profiles,
                actor,
                prepared -> true);
    }

    private static SignedControl signGrant(
            final TargetQuotaGrantControlRequest request,
            final ShardId shard,
            final ControlAuthorizationContext actor,
            final KeyPair keys,
            final long retryUntil) {
        final var reference = new ControlRef(
                Bytes.sha256(Bytes.utf8("target-scheduled-expiry-grant-operation")),
                PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()),
                0);
        final var body = new TargetQuotaGrantControlBody(shard, retryUntil, reference, request);
        return signControl(
                request.operationKind(),
                request.operationRequest(),
                reference.operationId(),
                body.logicalIdentity(),
                body.canonicalBytes(),
                retryUntil,
                shard,
                actor,
                keys);
    }

    private static SignedControl signMembership(
            final TargetMembershipControlRequest request,
            final ShardId shard,
            final ControlAuthorizationContext actor,
            final KeyPair keys,
            final byte[] operationId) {
        final var reference = new ControlRef(
                operationId,
                PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()),
                0);
        final var body = new TargetMembershipControlBody(
                shard, System.currentTimeMillis() + 600_000, reference, request);
        return signControl(
                request.operationKind(),
                request.operationRequest(),
                operationId,
                body.logicalIdentity(),
                body.canonicalBytes(),
                body.retryUntil(),
                shard,
                actor,
                keys);
    }

    private static SignedControl signControl(
            final ControlOperationKind kind,
            final ControlOperationRequest request,
            final byte[] operationId,
            final byte[] logicalIdentity,
            final byte[] canonicalBody,
            final long retryUntil,
            final ShardId shard,
            final ControlAuthorizationContext actor,
            final KeyPair keys) {
        final var mutation = SystemMutation.signed(
                shard,
                SystemMutationType.APPLY_SHARD_CONTROL,
                retryUntil,
                logicalIdentity,
                canonicalBody,
                AuthorIdentity.control(actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash())
                        .canonicalBytes(),
                1,
                keys.getPrivate());
        final var target = new ControlTargetRef(
                0,
                ControlTargetKind.SHARD,
                new ShardSubject(shard),
                mutation.systemMutationId(),
                mutation.mutationHash());
        final var prepared = PreparedControlOperation.prepare(
                operationId,
                kind,
                new ControlAuthor(actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash()),
                request,
                List.of(target),
                1,
                retryUntil,
                1,
                keys.getPrivate());
        return new SignedControl(prepared, mutation);
    }

    private static TargetAdmissionFixture appendTargetAdmission(
            final String bootstrap,
            final String topic,
            final String clusterId,
            final Uuid topicId,
            final ShardStore store,
            final TargetStoreBackend backend,
            final TargetQuotaScope scope,
            final TargetQuotaGrantActivation activation,
            final CanonicalScheduleIntent intent,
            final TargetMessageRecord scheduledMessage,
            final CanonicalTargetPartition physical,
            final com.nereusstream.delay.ownership.OwnerLease active,
            final OxiaOwnerLeaseStore leases,
            final KeyPair keys)
            throws Exception {
        final long now = System.currentTimeMillis();
        if (now >= scheduledMessage.expireAtEpochMs()) {
            throw new IllegalStateException("Target Admission fixture expired before its Claim was created");
        }
        final byte[] queueKey = TargetKeyCodec.state(physical.id());
        final var queue = TargetQueueState.decode(TargetValueEnvelope.decode(
                        store.get(ColumnFamily.META, queueKey), TargetQueueState.VALUE_TYPE)
                .payload());
        final var head = queue.domains().get(scheduledMessage.locator().domain().slot()).ordinaryHead();
        if (head == null || !scheduledMessage.locator().messageId().equals(head.messageId())) {
            throw new IllegalStateException("Target Admission fixture did not select its scheduled Message head");
        }
        final long deadline = Math.min(
                scheduledMessage.expireAtEpochMs() - 1, Math.addExact(now, 30_000));
        final long executionBytes = activation.allocation().accounting().accountedPublishBytes(
                AdapterKind.PULSAR,
                intent.inlinePayload().length,
                intent.adapterMetadata().canonicalBytes().length);
        final var owner = new OwnerIdentity(
                Bytes.utf8("kafka-target-worker-deployment"),
                Bytes.utf8("kafka-target-worker-host-run"),
                active.ownerEpoch(),
                Bytes.sha256(active.leaseToken()));
        final var claimStore = new TargetClaimStore(
                backend, scope, LINEAGE, 1, (kind, delta) -> {});
        final var claimPlan = claimStore.prepareClaim(
                KafkaClientArtifactTargetWorkerSourceSmoke.budget(),
                head,
                owner,
                now,
                deadline,
                executionBytes,
                Bytes.sha256(Bytes.utf8("kafka-target-publish-admission-local-claim")));
        claimStore.commit(
                claimPlan,
                KafkaClientArtifactTargetWorkerSourceSmoke.ownerCommitAuthority(leases, active));
        final TargetClaimRecord claim = claimPlan.claim();

        final long retryUntil = Math.addExact(System.currentTimeMillis(), 60_000);
        final int attemptNo = claim.work().candidateAttemptNo();
        final byte[] attemptId = SystemMutation.computePublishAttemptLogicalIdentity(
                claim.claimId(),
                claim.work().locator().messageId(),
                Integer.toUnsignedLong(claim.work().locator().generation()),
                Integer.toUnsignedLong(attemptNo));
        final var obligation = new AttemptObligationRef(
                attemptId,
                claim.work().locator().generation(),
                AttemptLedgerState.PUBLISHING,
                KeyCodec.inflight((byte) 2, owner.ownerEpoch(), attemptId));
        final long evidenceNow = System.currentTimeMillis();
        final var decisionTime = new TrustedUtcIntervalEvidence(
                Math.max(claim.work().retryEligibilityAtEpochMs(), Math.max(0, evidenceNow - 10_000)),
                Math.addExact(evidenceNow, 10_000),
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("kafka-target-publish-admission-clock"),
                1,
                1,
                Math.max(0, System.nanoTime()),
                Bytes.sha256(Bytes.utf8("kafka-target-publish-admission-clock-evidence")),
                0,
                null);
        final long[] reserved = new long[com.nereusstream.delay.protocol.CapacityDimension.COUNT];
        reserved[CapacityDimension.RESULT_BYTES.wireValue() - 1] = 128;
        final var body = new TargetPublishAdmissionBody(
                scope.shard(),
                retryUntil,
                owner,
                claim.storeIncarnation(),
                claim.claimId(),
                claim.work().locator(),
                attemptNo,
                attemptId,
                obligation,
                claim.executionBytes(),
                new CapacityVector(reserved),
                CapacityVector.empty(),
                decisionTime);
        final var author = AuthorIdentity.owner(
                owner.deploymentId(), owner.workerRunId(), owner.ownerEpoch(), owner.leaseFencingDigest());
        final var mutation = SystemMutation.signed(
                scope.shard(),
                SystemMutationType.TARGET_PUBLISH_ADMISSION,
                retryUntil,
                attemptId,
                body.canonicalBytes(),
                author.canonicalBytes(),
                1,
                keys.getPrivate());
        final var metadata = KafkaClientArtifactTargetWorkerSourceSmoke.produce(
                bootstrap,
                topic,
                scope.shard().partition(),
                evidenceNow,
                attemptId,
                mutation.encodeFrame());
        requireOffset(metadata.offset(), 4, "Target Publish Admission");
        final KafkaSourcePosition source = position(metadata, scope.shard(), clusterId, topicId);
        return new TargetAdmissionFixture(new SourceReplayMutation(mutation, source, null, null), claim, attemptId);
    }

    private record TargetAdmissionFixture(SourceReplayMutation sourceEntry, TargetClaimRecord claim, byte[] attemptId) {
        private TargetAdmissionFixture {
            attemptId = Bytes.copy(attemptId);
        }

        @Override
        public byte[] attemptId() {
            return Bytes.copy(attemptId);
        }

        private SystemMutation mutation() {
            return sourceEntry.mutation();
        }

        private KafkaSourcePosition position() {
            return (KafkaSourcePosition) sourceEntry.position();
        }
    }

    private static TrustedUtcIntervalEvidence expiryEvidence(final long now) {
        return new TrustedUtcIntervalEvidence(
                Math.max(0, now - 1),
                now,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("kafka-target-scheduled-expiry-clock"),
                1,
                1,
                Math.max(0, System.nanoTime()),
                Bytes.sha256(Bytes.utf8("kafka-target-scheduled-expiry-clock-evidence")),
                0,
                null);
    }

    private static KafkaSourcePosition position(
            final org.apache.kafka.clients.producer.RecordMetadata metadata,
            final ShardId shard,
            final String clusterId,
            final Uuid topicId) {
        if (metadata.timestamp() < 0) {
            throw new IllegalStateException("Kafka LogAppendTime is missing for Target source fixture");
        }
        return new KafkaSourcePosition(
                shard,
                clusterId,
                toUuid(topicId),
                metadata.offset(),
                null,
                metadata.timestamp());
    }

    private static void requireOffset(final long actual, final long expected, final String label) {
        if (actual != expected) {
            throw new IllegalStateException(label + " source fixture offset mismatch: expected " + expected
                    + " but received " + actual);
        }
    }

    private static UUID toUuid(final Uuid value) {
        return new UUID(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }

    private static byte[] vector(final String resource, final String key) {
        final var properties = new java.util.Properties();
        try (var input = KafkaClientArtifactTargetScheduledExpirySmoke.class
                .getClassLoader()
                .getResourceAsStream(resource)) {
            if (input == null) {
                throw new IllegalStateException("missing locked protocol vector resource " + resource);
            }
            properties.load(input);
        } catch (java.io.IOException failure) {
            throw new IllegalStateException("cannot read locked protocol vector resource " + resource, failure);
        }
        final String encoded = properties.getProperty(key);
        if (encoded == null) {
            throw new IllegalStateException("missing locked protocol vector " + key);
        }
        return java.util.HexFormat.of().parseHex(encoded);
    }

    private record SignedControl(PreparedControlOperation prepared, SystemMutation mutation) {}
}
