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
import com.nereusstream.delay.protocol.TargetQuotaGrant;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlBody;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
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
import com.nereusstream.delay.runtime.TargetMessageRecord;
import com.nereusstream.delay.runtime.TargetQuotaGrantControlVerifier;
import com.nereusstream.delay.runtime.TargetQuotaGrantStore;
import com.nereusstream.delay.runtime.TargetStoreBootstrap;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
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
            final Uuid topicId)
            throws Exception {
        final ShardId shard = new ShardId(com.nereusstream.delay.protocol.RouteIncarnation.random(), 0);
        final var scope = new TargetQuotaScope(
                shard, Bytes.sha256(Bytes.utf8("target-scheduled-expiry-tenant")), null);
        final String groupId = "nereus-delay-target-expiry-" + UUID.randomUUID();
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
        final long expireAt = Math.addExact(scheduleBuildTime, 2_000);
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
            try (var resources = new SharedRocksDbResources(config);
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
                final var expiryProof = new AtomicReference<TrustedUtcIntervalEvidence>();
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
                                        expiryProof),
                                entry -> { throw new AssertionError("expiry did not resolve Target Close"); },
                                entry -> { throw new AssertionError("expiry did not resolve membership"); },
                                KafkaClientArtifactTargetWorkerSourceSmoke.ownerCommitAuthority(leases, active),
                                KafkaClientArtifactTargetWorkerSourceSmoke.ownerReadAuthority(leases, active),
                                entry -> { throw new AssertionError("expiry source was a Client Command"); }),
                        new TargetSourceApplyRuntime.Limits(2048, 16L << 20, 60_000_000_000L, 16, 1),
                        System::nanoTime);
                final var consumer = KafkaClientArtifactTargetWorkerSourceSmoke.newSourceConsumer(
                        bootstrap,
                        groupId,
                        clusterId,
                        topic,
                        toUuid(topicId),
                        shard,
                        KafkaClientArtifactTargetWorkerSourceSmoke.AckMode.NO_INJECTION);
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
                    worker.configureMessageExpiryMaintenance(
                            appender,
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
                    final long remaining = scheduledMessage.expireAtEpochMs() - System.currentTimeMillis();
                    if (remaining > 0) {
                        TimeUnit.MILLISECONDS.sleep(remaining);
                    }
                    host = TargetWorkerHostRuntime.start(
                            workerClasses,
                            resources,
                            List.of(worker),
                            new SchedulerBudget(64, 4L << 20, TimeUnit.SECONDS.toNanos(10)),
                            Duration.ofMillis(50),
                            failure -> {});
                    if (!expiryRequest.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Target Host did not request expiry discovery");
                    }
                    final var turn = KafkaClientArtifactTargetWorkerSourceSmoke.runUntilAppliedByHost(host);
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
                            + " are explicit test fixtures.");
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
            final AtomicReference<TrustedUtcIntervalEvidence> expectedEvidence) {
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
