package io.nereusstream.delay.transport;

import io.nereusstream.delay.ownership.RouteWorkerAssignmentCoordinator;
import io.nereusstream.delay.ownership.SourceAcknowledgement;
import io.nereusstream.delay.ownership.SourceRecordConsumer;
import io.nereusstream.delay.ownership.SourceReplayRecord;
import io.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import io.nereusstream.delay.ownership.OxiaSyncWorkerAssignmentBackend;
import io.nereusstream.delay.ownership.WorkerAssignment;
import io.nereusstream.delay.ownership.WorkerAssignmentAuthority;
import io.nereusstream.delay.ownership.WorkerAssignmentCoordinator;
import io.nereusstream.delay.protocol.ActivationBarrierV1;
import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CapacityDimensionV1;
import io.nereusstream.delay.protocol.CapacityVectorV1;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.IngressCredentialBindingRefV1;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.KafkaIngressRouteResourceV1;
import io.nereusstream.delay.protocol.KafkaMetadataV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProtocolTupleV1;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.QuotaGrantRefV1;
import io.nereusstream.delay.protocol.RetryPolicyRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.RouteLifecycleV1;
import io.nereusstream.delay.protocol.RoutePartitionPolicyV1;
import io.nereusstream.delay.protocol.RouteSnapshotV1;
import io.nereusstream.delay.protocol.RoutingHashVersionV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.route.OxiaRouteAuthoritySession;
import io.nereusstream.delay.route.OxiaSignedRouteSnapshotProvider;
import io.nereusstream.delay.route.OxiaSignedRouteSnapshotPublisher;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import io.nereusstream.delay.semantic.RouteSelectionHint;
import io.nereusstream.delay.store.WorkerLoadVector;
import io.nereusstream.delay.store.WorkerPlacementPolicy;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerResourceGuard;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.clients.consumer.GuardedConsumerRecords;
import org.apache.kafka.clients.consumer.GuardedFetchEvidence;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.clients.producer.GuardedProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Real Kafka proof from guarded Fetch evidence to a signed Route and source
 * assignment. The Oxia authority is deliberately required: this smoke does
 * not substitute an in-memory Route or assignment authority for the durable
 * publication/acceptance boundary.
 */
public final class KafkaClientArtifactRouteWorkerSmoke {
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(250);

    private KafkaClientArtifactRouteWorkerSmoke() {
    }

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException("usage: <bootstrap-server> <route-topic>");
        }
        final String oxiaEndpoint = configuredRequired("NEREUS_DELAY_OXIA_ENDPOINT");
        final String bootstrap = arguments[0];
        final String topic = arguments[1] + "-" + UUID.randomUUID();
        final Map<String, Object> adminConfiguration = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);

        try (Admin admin = Admin.create(adminConfiguration)) {
            ensureTopic(admin, topic);
            final String clusterId = admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
            final Uuid topicId = describe(admin, topic).topicId();
            final UUID nativeTopicId = toUuid(topicId);
            final RouteIncarnation routeIncarnation = RouteIncarnation.random();
            final ShardId shard = new ShardId(routeIncarnation, 0);
            final PreparedCommand beforeRoute = command(shard, "route-before");
            final PreparedCommand afterRoute = command(shard, "route-after");
            append(bootstrap, clusterId, topic, topicId, beforeRoute, 0);

            final GuardedFetchEvidence fetchEvidence = fetchEvidence(bootstrap, clusterId, topic, nativeTopicId,
                    shard);
            final long barrierOffset = Math.addExact(fetchEvidence.lastRecordOffset(), 1);
            if (fetchEvidence.firstRecordOffset() != 0 || fetchEvidence.lastRecordOffset() != 0
                    || fetchEvidence.lastStableOffset() < barrierOffset) {
                throw new IllegalStateException("Kafka Fetch proof did not establish the one-record LSO barrier: "
                        + fetchEvidence);
            }

            final KeyPair signingKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final RouteSelectionHint hint = new RouteSelectionHint(AdapterKindV1.KAFKA, Bytes.utf8("primary"));
            final AuthenticatedTenantContext tenant = new AuthenticatedTenantContext(
                    bytes(32, 1), bytes(32, 2), bytes(32, 3));
            final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
            final String routePrefix = "nereus-delay/kafka-route-worker/" + UUID.randomUUID();
            final String assignmentPrefix = "nereus-delay/kafka-route-worker-assignment/" + UUID.randomUUID();
            try (OxiaRouteAuthoritySession publisherSession = OxiaRouteAuthoritySession.connect(
                         oxiaEndpoint, namespace, "nereus-delay-kafka-route-publisher-" + UUID.randomUUID(),
                         Duration.ofSeconds(15), routePrefix);
                 OxiaRouteAuthoritySession providerSession = OxiaRouteAuthoritySession.connect(
                         oxiaEndpoint, namespace, "nereus-delay-kafka-route-provider-" + UUID.randomUUID(),
                         Duration.ofSeconds(15), routePrefix);
                 OxiaSyncOwnerLeaseBackend.ClientHandle assignmentHandle =
                         OxiaSyncOwnerLeaseBackend.connectUnchecked(
                                 oxiaEndpoint, namespace, "nereus-delay-kafka-route-assignment-" + UUID.randomUUID(),
                                 Duration.ofSeconds(15), assignmentPrefix)) {
                final RouteSnapshotV1 snapshot = routeSnapshot(clusterId, topic, nativeTopicId, routeIncarnation,
                        fetchEvidence, signingKeys);
                final OxiaSignedRouteSnapshotPublisher publisher = new OxiaSignedRouteSnapshotPublisher(
                        publisherSession, routePrefix, signingKeys.getPublic());
                final OxiaSignedRouteSnapshotProvider provider = new OxiaSignedRouteSnapshotProvider(
                        providerSession, routePrefix, signingKeys.getPublic(), System::currentTimeMillis);
                publisher.publish(hint, snapshot, 0);
                provider.refresh().toCompletableFuture().join();

                final WorkerAssignmentAuthority authority = new OxiaSyncWorkerAssignmentBackend(assignmentHandle,
                        assignmentPrefix);
                final RouteWorkerAssignmentCoordinator coordinator = new RouteWorkerAssignmentCoordinator(provider,
                        new WorkerAssignmentCoordinator(new WorkerPlacementPolicy(
                                new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)), authority));
                final RouteWorkerAssignmentCoordinator.RoutePlacementResult placement = coordinator.placeActive(
                        tenant, hint, placementRequest(System.currentTimeMillis()));
                final WorkerAssignment publication = placement.publication().assignment();
                final WorkerAssignment accepted = coordinator.requireAccepted(tenant,
                        placement.publication().revision(), publication);
                requireRouteAssignment(accepted, snapshot, clusterId, nativeTopicId, barrierOffset);

                append(bootstrap, clusterId, topic, topicId, afterRoute, barrierOffset);
                final SourceObservation observation = pollAfterBarrier(bootstrap, clusterId, topic, nativeTopicId,
                        shard, barrierOffset, afterRoute);
                requireCommittedOffset(admin, observation.groupId(), topic, 0, barrierOffset + 1);
                if (!authority.withdraw(placement.publication())) {
                    throw new IllegalStateException("Kafka Route Worker assignment was not withdrawn exactly");
                }
                provider.close();
                System.out.println("Kafka signed Route -> guarded Fetch barrier -> Oxia Worker assignment smoke "
                        + "passed: fetch=v" + fetchEvidence.requestVersion() + ", lso="
                        + fetchEvidence.lastStableOffset() + ", routeRevision=" + placement.routeRevision()
                        + ", assignmentRevision=" + placement.publication().revision() + ", barrierOffset="
                        + barrierOffset + ", sourceOffset=" + observation.position().offset()
                        + ", commitSync ACK");
            }
        }
    }

    private static GuardedFetchEvidence fetchEvidence(final String bootstrap, final String clusterId,
                                                       final String topic, final UUID topicId,
                                                       final ShardId shard) {
        final String groupId = "nereus-delay-route-barrier-" + UUID.randomUUID();
        final GuardedConsumer<byte[], byte[]> consumer = KafkaClientArtifactSourceConsumerFactory.create(
                consumerConfiguration(bootstrap, groupId), clusterId, topic, topicId, shard.partition());
        final TopicPartition topicPartition = new TopicPartition(topic, shard.partition());
        final ConsumerResourceGuard guard = new ConsumerResourceGuard(clusterId, topic,
                new Uuid(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits()), shard.partition());
        try {
            consumer.assign(List.of(topicPartition));
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (System.nanoTime() < deadline) {
                final GuardedConsumerRecords<byte[], byte[]> records = consumer.pollGuarded(POLL_TIMEOUT);
                final GuardedFetchEvidence evidence = KafkaClientArtifactFetchEvidence.requireBatch(records, guard);
                if (evidence != null) {
                    return evidence;
                }
            }
            throw new IllegalStateException("Kafka guarded Fetch evidence did not become visible");
        } finally {
            consumer.close();
        }
    }

    private static SourceObservation pollAfterBarrier(final String bootstrap, final String clusterId,
                                                       final String topic, final UUID topicId, final ShardId shard,
                                                       final long barrierOffset, final PreparedCommand expected) {
        final String groupId = "nereus-delay-route-source-" + UUID.randomUUID();
        final GuardedConsumer<byte[], byte[]> consumer = KafkaClientArtifactSourceConsumerFactory.create(
                consumerConfiguration(bootstrap, groupId), clusterId, topic, topicId, shard.partition());
        final KafkaClientArtifactSourceRecordConsumer source = new KafkaClientArtifactSourceRecordConsumer(consumer,
                clusterId, topicId, shard, topic, POLL_TIMEOUT);
        try {
            consumer.seek(new TopicPartition(topic, shard.partition()), barrierOffset);
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (System.nanoTime() < deadline) {
                final Optional<SourceRecordConsumer.PolledSourceRecord> polled = source.poll();
                if (polled.isEmpty()) {
                    continue;
                }
                if (!(polled.get().entry() instanceof SourceReplayRecord replay)
                        || !expected.equals(replay.command())
                        || !(replay.position() instanceof io.nereusstream.delay.protocol.KafkaSourcePosition position)
                        || position.offset() != barrierOffset) {
                    throw new IllegalStateException("Kafka source did not start at the signed Route barrier");
                }
                requireAcked(polled.get().acknowledgement().acknowledge(polled.get().entry(), null));
                return new SourceObservation(groupId, polled.get(), position);
            }
            throw new IllegalStateException("Kafka source record after Route barrier did not become visible");
        } catch (RuntimeException | Error failure) {
            source.close();
            throw failure;
        } finally {
            source.close();
        }
    }

    private static RouteSnapshotV1 routeSnapshot(final String clusterId, final String topic,
                                                  final UUID topicId, final RouteIncarnation incarnation,
                                                  final GuardedFetchEvidence evidence, final KeyPair signingKeys) {
        final long now = System.currentTimeMillis();
        final BrokerResourceIdentityV1 broker = BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1(clusterId, topicId));
        final long nextOffsetExclusive = Math.addExact(evidence.lastRecordOffset(), 1);
        final byte[] guardDigest = Bytes.sha256(Bytes.utf8("nereus-delay-kafka-route-fetch-proof-v1\0"),
                evidence.fetchResponseBodySha256());
        final RoutePartitionPolicyV1 policy = new RoutePartitionPolicyV1(0,
                ActivationBarrierV1.kafka(broker, 0, nextOffsetExclusive, evidence.lastStableOffset()),
                zeroQuota(), 1, guardDigest);
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(now - 100, now,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("kafka-route-clock"),
                1, 1, 1, Bytes.sha256(Bytes.utf8("kafka-route-issued-at")), 0, null);
        return RouteSnapshotV1.create(incarnation, bytes(32, 1), bytes(32, 2), RouteLifecycleV1.ACTIVE_FOR_NEW,
                now + 30_000, new KafkaIngressRouteResourceV1(clusterId, topic, topicId, 1),
                RoutingHashVersionV1.ROUTING_HASH_V1,
                new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1), 1, List.of(policy),
                100, 200, 1024, 4096, 10, 8192, 500, now - 1_000, now + 60_000,
                new IngressCredentialBindingRefV1(bytes(32, 40), 1, bytes(32, 41), bytes(32, 42), bytes(32, 43)),
                Bytes.sha256(Bytes.utf8("kafka-route-prerequisite")), issuedAt, 1, signingKeys.getPrivate());
    }

    private static RouteWorkerAssignmentCoordinator.PlacementRequest placementRequest(final long now) {
        return new RouteWorkerAssignmentCoordinator.PlacementRequest(0,
                Bytes.sha256(Bytes.utf8("kafka-route-worker-assignment")), 1,
                Bytes.sha256(Bytes.utf8("kafka-route-worker-capacity")), 1,
                List.of(new WorkerPlacementPolicy.WorkerCandidate("kafka-route-worker", capacity(2),
                        CapacityVectorV1.empty(), 0, 16, 0, 16, WorkerLoadVector.empty(), WorkerLoadVector.empty(),
                        now, true, 0)), capacity(1), CapacityVectorV1.empty(), CapacityVectorV1.empty(), null,
                now, 0, 0);
    }

    private static void requireRouteAssignment(final WorkerAssignment assignment, final RouteSnapshotV1 snapshot,
                                               final String clusterId, final UUID topicId,
                                               final long barrierOffset) {
        if (!assignment.routeBound() || !java.util.Arrays.equals(snapshot.snapshotDigest(),
                assignment.routeSnapshotDigest()) || !(assignment.sourceAssignment().activationBarrier()
                instanceof KafkaActivationBarrier barrier) || !clusterId.equals(barrier.authenticatedClusterId())
                || !topicId.equals(barrier.nativeTopicUuid()) || barrier.exclusiveOffset() != barrierOffset) {
            throw new IllegalStateException("Oxia Worker assignment did not retain the signed Kafka Route barrier");
        }
    }

    private static void append(final String bootstrap, final String clusterId, final String topic,
                               final Uuid topicId, final PreparedCommand command, final long expectedOffset)
            throws Exception {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ProducerConfig.ACKS_CONFIG, "all");
        configuration.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configuration.put("allow.auto.create.topics", false);
        configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(configuration,
                new ByteArraySerializer(), new ByteArraySerializer())) {
            final GuardedProducer<byte[], byte[]> guarded = (GuardedProducer<byte[], byte[]>) producer;
            final var metadata = guarded.sendGuarded(new ProducerRecord<>(topic, 0, null,
                    io.nereusstream.delay.protocol.CommandCodec.encodeFrameV1(command)),
                    new org.apache.kafka.clients.producer.ProducerResourceGuard(clusterId, topic, topicId, 0))
                    .get(10, TimeUnit.SECONDS);
            if (metadata.recordMetadata().offset() != expectedOffset) {
                throw new IllegalStateException("Kafka Route smoke append offset mismatch: expected="
                        + expectedOffset + ", actual=" + metadata.recordMetadata().offset());
            }
        }
    }

    private static Map<String, Object> consumerConfiguration(final String bootstrap, final String groupId) {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configuration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configuration.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        configuration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configuration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        configuration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return configuration;
    }

    private static PreparedCommand command(final ShardId shard, final String identity) {
        final ProfileRefV1 destination = new ProfileRefV1(Bytes.utf8("destination-" + identity), 1,
                Bytes.sha256(Bytes.utf8("destination-semantic-" + identity)), ProfileKindV1.DESTINATION);
        final RetryPolicyRefV1 retryPolicy = new RetryPolicyRefV1(Bytes.utf8("retry-" + identity), 1,
                Bytes.sha256(Bytes.utf8("retry-semantic-" + identity)));
        final long deliverAt = System.currentTimeMillis() + 1_000;
        final io.nereusstream.delay.protocol.ScheduleIntentV1 intent =
                io.nereusstream.delay.protocol.ScheduleIntentV1.create(destination, retryPolicy, deliverAt,
                        deliverAt + 10_000, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0],
                        Bytes.utf8("source-" + identity), null,
                        AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null);
        return PreparedCommand.scheduleV1(shard, intent, deliverAt + 20_000);
    }

    private static QuotaGrantRefV1 zeroQuota() {
        return new QuotaGrantRefV1(bytes(32, 20), 1, new PublishAdmissionBody.ChargeVector(
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
    }

    private static CapacityVectorV1 capacity(final long dbInstances) {
        final long[] values = new long[CapacityDimensionV1.COUNT];
        values[CapacityDimensionV1.DB_INSTANCES.wireValue() - 1] = dbInstances;
        return new CapacityVectorV1(values);
    }

    private static void requireAcked(final SourceAcknowledgement.AcknowledgementResult result) {
        if (result.disposition() != SourceAcknowledgement.Disposition.ACKED) {
            throw new IllegalStateException("Kafka Route source record was not ACKED: " + result.disposition(),
                    result.failure());
        }
    }

    private static void requireCommittedOffset(final Admin admin, final String groupId, final String topic,
                                               final int partition, final long expected) throws Exception {
        final TopicPartition topicPartition = new TopicPartition(topic, partition);
        final OffsetAndMetadata offset = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata()
                .get(10, TimeUnit.SECONDS).get(topicPartition);
        if (offset == null || offset.offset() != expected) {
            throw new IllegalStateException("Kafka Route source group offset mismatch: expected=" + expected
                    + ", actual=" + (offset == null ? "missing" : offset.offset()));
        }
    }

    private static void ensureTopic(final Admin admin, final String topic) throws Exception {
        try {
            if (describe(admin, topic) != null) {
                return;
            }
        } catch (Exception missing) {
            // Create below.
        }
        final NewTopic newTopic = new NewTopic(topic, 1, (short) 3);
        newTopic.configs(Map.of("message.timestamp.type", "LogAppendTime"));
        admin.createTopics(List.of(newTopic)).all().get(10, TimeUnit.SECONDS);
        for (int attempt = 0; attempt < 30; attempt++) {
            try {
                if (describe(admin, topic) != null) {
                    return;
                }
            } catch (Exception ignored) {
                // Retry while metadata converges.
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new IllegalStateException("Kafka Route topic metadata did not converge");
    }

    private static TopicDescription describe(final Admin admin, final String topic) throws Exception {
        return admin.describeTopics(List.of(topic)).allTopicNames().get(10, TimeUnit.SECONDS).get(topic);
    }

    private static UUID toUuid(final Uuid value) {
        return new UUID(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }

    private static String configuredRequired(final String name) {
        final String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " is required for the real Route authority smoke");
        }
        return value;
    }

    private static String configured(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private record SourceObservation(String groupId, SourceRecordConsumer.PolledSourceRecord record,
                                     io.nereusstream.delay.protocol.KafkaSourcePosition position) {
    }

}
