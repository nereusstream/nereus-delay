package com.nereusstream.delay.store;

import com.nereusstream.delay.ownership.LegacyCheckpointTailReplayer;
import com.nereusstream.delay.ownership.ReplayTurnBudget;
import com.nereusstream.delay.ownership.SourceAcknowledgement;
import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.ownership.SourceRecordConsumer;
import com.nereusstream.delay.ownership.SourceReplayOutcome;
import com.nereusstream.delay.ownership.SourceReplayRecord;
import com.nereusstream.delay.ownership.SourceReplaySuccessor;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.KafkaMetadata;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.DelayShardConfig;
import com.nereusstream.delay.transport.KafkaClientArtifactRecoverySourceCursor;
import com.nereusstream.delay.transport.KafkaClientArtifactSourceConsumerFactory;
import com.nereusstream.delay.transport.KafkaClientArtifactSourceRecordConsumer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.GuardedProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerResourceGuard;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** Real Kafka verification of bounded legacy checkpoint-tail replay into an isolated Store copy. */
public final class KafkaLegacyCheckpointReplaySmoke {
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(250);
    private static final CheckpointManifestLimits MANIFEST_LIMITS =
            new CheckpointManifestLimits(1_024, 1L << 30, 1L << 28, 4_096, 1 << 20, 256, 4_096);

    private KafkaLegacyCheckpointReplaySmoke() {}

    public static void run(final String bootstrap, final Admin admin, final String clusterId, final ShardId shard)
            throws Exception {
        final String topic = "nereus-delay-b6-legacy-replay-" + UUID.randomUUID();
        ensureTopic(admin, topic);
        final Uuid nativeTopicId = describe(admin, topic).topicId();
        final UUID topicId = toUuid(nativeTopicId);
        final PreparedCommand checkpointCommand = command(shard, "checkpoint");
        final PreparedCommand cutCommand = command(shard, "cut");
        final PreparedCommand afterCutCommand = command(shard, "after-cut");
        final List<Long> producedOffsets =
                produce(
                        bootstrap,
                        clusterId,
                        topic,
                        nativeTopicId,
                        List.of(checkpointCommand, cutCommand, afterCutCommand));
        if (producedOffsets.size() != 3
                || producedOffsets.get(1) != producedOffsets.get(0) + 1
                || producedOffsets.get(2) <= producedOffsets.get(1)) {
            throw new IllegalStateException(
                    "Kafka B6 fixture did not produce a strict checkpoint, cut, and tail order");
        }

        final Path temporaryRoot = Files.createTempDirectory("nereus-delay-b6-kafka-replay-");
        try {
            verifyReplay(
                    bootstrap,
                    admin,
                    clusterId,
                    topic,
                    topicId,
                    shard,
                    checkpointCommand,
                    cutCommand,
                    producedOffsets,
                    temporaryRoot);
        } finally {
            deleteTree(temporaryRoot);
        }
    }

    private static void verifyReplay(
            final String bootstrap,
            final Admin admin,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final ShardId shard,
            final PreparedCommand checkpointCommand,
            final PreparedCommand cutCommand,
            final List<Long> producedOffsets,
            final Path temporaryRoot)
            throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(temporaryRoot.resolve("store-root"));
        final Path checkpointPath = temporaryRoot.resolve("legacy-checkpoint");
        final byte[] checkpointId = uuidBytes(UUID.randomUUID());
        final String activeGroup = "nereus-delay-b6-active-" + UUID.randomUUID();
        final SourceAssignment assignment = new SourceAssignment(
                shard,
                Bytes.sha256(Bytes.utf8("kafka-b6-assignment"), Bytes.utf8(topic)),
                1,
                new KafkaActivationBarrier(shard, clusterId, topicId, 0));
        final Path activePointer = config.rootPath()
                .resolve("shards")
                .resolve(shard.routeIncarnation().uuid().toString())
                .resolve(Integer.toUnsignedString(shard.partition()))
                .resolve("ACTIVE");
        byte[] activePointerBefore = null;
        KafkaSourcePosition checkpointPosition = null;
        KafkaSourcePosition sourceCutPosition = null;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore originalStore = ShardStore.open(config, shard, resources)) {
            final DelayShard originalApplier = new DelayShard(originalStore, DelayShardConfig.defaults());
            try (KafkaClientArtifactSourceRecordConsumer activeSource = source(
                    bootstrap, activeGroup, clusterId, topic, topicId, shard)) {
                final SourceRecordConsumer.PolledSourceRecord checkpointRecord =
                        pollExpected(activeSource, checkpointCommand);
                final SourceReplayRecord checkpointReplay = (SourceReplayRecord) checkpointRecord.entry();
                checkpointPosition = (KafkaSourcePosition) checkpointReplay.position();
                if (checkpointPosition.offset() != producedOffsets.get(0)) {
                    throw new IllegalStateException("Kafka B6 checkpoint entry differs from producer receipt");
                }
                applyThenAcknowledge(originalApplier, checkpointRecord, "checkpoint source record");
                requireCommittedOffset(admin, activeGroup, topic, shard.partition(), checkpointPosition.offset() + 1);

                originalStore.createCheckpoint(checkpointPath, checkpointId);
                final CheckpointManifest manifest = manifestFor(
                        checkpointPath, shard, originalStore, checkpointId, checkpointPosition);
                activePointerBefore = Files.readAllBytes(activePointer);

                final SourceRecordConsumer.PolledSourceRecord cutRecord = pollExpected(activeSource, cutCommand);
                final SourceReplayRecord cutReplay = (SourceReplayRecord) cutRecord.entry();
                sourceCutPosition = (KafkaSourcePosition) cutReplay.position();
                if (sourceCutPosition.offset() != producedOffsets.get(1)) {
                    throw new IllegalStateException("Kafka B6 cut entry differs from producer receipt");
                }
                applyThenAcknowledge(originalApplier, cutRecord, "source cut record");
                requireCommittedOffset(admin, activeGroup, topic, shard.partition(), sourceCutPosition.offset() + 1);
                if (!samePosition(originalStore.appliedShardLogPosition(), sourceCutPosition)) {
                    throw new IllegalStateException("active legacy Store did not reach the acknowledged source cut");
                }

                final SourceRecordConsumer.CheckpointCut protectedCut = activeSource.checkpointCut(sourceCutPosition);
                protectedCut.requireCurrent();
                final String recoveryGroup = activeGroup;
                final long startOffset = Math.addExact(checkpointPosition.offset(), 1);
                final KeyPair verificationKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
                try (ShardStore.LegacyCheckpointReplayCopy replayCopy = ShardStore.openLegacyCheckpointReplayCopy(
                        config, shard, resources, checkpointPath, manifest, MANIFEST_LIMITS);
                        KafkaClientArtifactRecoverySourceCursor recovery = new KafkaClientArtifactRecoverySourceCursor(
                                recoveryConsumer(
                                        bootstrap, recoveryGroup, clusterId, topic, topicId, shard),
                                assignment,
                                topic,
                                startOffset,
                                POLL_TIMEOUT)) {
                    final LegacyCheckpointTailReplayer.Result result = LegacyCheckpointTailReplayer.replay(
                            replayCopy,
                            assignment,
                            SourceReplaySuccessor.strictKafka(),
                            protectedCut,
                            recovery,
                            store -> new DelayShard(store, DelayShardConfig.defaults()),
                            verificationKeys.getPublic(),
                            System::currentTimeMillis,
                            new ReplayTurnBudget(8, 1 << 20, TimeUnit.SECONDS.toNanos(30)));
                    if (result.status() != LegacyCheckpointTailReplayer.Status.EXACT_CUT_REACHED
                            || result.recordsApplied() != 1
                            || !samePosition(result.checkpointPosition(), checkpointPosition)
                            || !samePosition(result.sourceCut(), sourceCutPosition)
                            || !samePosition(result.appliedThrough(), sourceCutPosition)
                            || !samePosition(replayCopy.store().appliedShardLogPosition(), sourceCutPosition)) {
                        throw new IllegalStateException("Kafka legacy checkpoint replay did not reach the exact cut");
                    }
                    protectedCut.requireCurrent();
                    requireCommittedOffset(
                            admin, recoveryGroup, topic, shard.partition(), sourceCutPosition.offset() + 1);
                }

                final LegacyCheckpointImageInspector.ImageProof imageProof = LegacyCheckpointImageInspector.inspect(
                        checkpointPath, shard, manifest, MANIFEST_LIMITS);
                if (!samePosition(imageProof.appliedSourcePosition(), checkpointPosition)
                        || !java.util.Arrays.equals(activePointerBefore, Files.readAllBytes(activePointer))) {
                    throw new IllegalStateException("legacy replay changed the source checkpoint or ACTIVE pointer");
                }
                activeSource.checkpointCut(sourceCutPosition).requireCurrent();
                System.out.println("Kafka B6 legacy checkpoint replay passed: topicId=" + topicId
                        + ", checkpointOffset=" + checkpointPosition.offset()
                        + ", sourceCutOffset=" + sourceCutPosition.offset()
                        + ", postCutOffset=" + producedOffsets.get(2)
                        + ", committedOffsetAfterReplay=" + (sourceCutPosition.offset() + 1));
            }
        }
    }

    private static KafkaClientArtifactSourceRecordConsumer source(
            final String bootstrap,
            final String groupId,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final ShardId shard) {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configuration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configuration.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        configuration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configuration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        configuration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return new KafkaClientArtifactSourceRecordConsumer(
                KafkaClientArtifactSourceConsumerFactory.create(
                        configuration, clusterId, topic, topicId, shard.partition()),
                clusterId,
                topicId,
                shard,
                topic,
                POLL_TIMEOUT);
    }

    private static GuardedConsumer<byte[], byte[]> recoveryConsumer(
            final String bootstrap,
            final String groupId,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final ShardId shard) {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configuration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configuration.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        configuration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configuration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        configuration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return KafkaClientArtifactSourceConsumerFactory.create(
                configuration, clusterId, topic, topicId, shard.partition());
    }

    private static SourceRecordConsumer.PolledSourceRecord pollExpected(
            final KafkaClientArtifactSourceRecordConsumer source, final PreparedCommand expected) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            final Optional<SourceRecordConsumer.PolledSourceRecord> polled = source.poll();
            if (polled.isPresent()) {
                if (!(polled.get().entry() instanceof SourceReplayRecord replay)
                        || !replay.command().equals(expected)) {
                    throw new IllegalStateException("Kafka B6 source returned an unexpected command");
                }
                return polled.get();
            }
        }
        throw new IllegalStateException("Kafka B6 source record did not become visible");
    }

    private static void applyThenAcknowledge(
            final DelayShard applier,
            final SourceRecordConsumer.PolledSourceRecord polled,
            final String label) {
        final SourceReplayRecord replay = (SourceReplayRecord) polled.entry();
        final var result = applier.apply(replay.command(), replay.position());
        final SourceAcknowledgement.AcknowledgementResult acknowledged = polled.acknowledgement()
                .acknowledge(replay, SourceReplayOutcome.command(replay.position(), result));
        if (acknowledged.disposition() != SourceAcknowledgement.Disposition.ACKED) {
            throw new IllegalStateException(label + " did not receive a durable Kafka ACK", acknowledged.failure());
        }
    }

    private static CheckpointManifest manifestFor(
            final Path image,
            final ShardId shard,
            final ShardStore store,
            final byte[] checkpointId,
            final KafkaSourcePosition sourcePosition) {
        final List<CheckpointManifest.FileEntry> files = CheckpointFileInventory
                .collect(image, MANIFEST_LIMITS)
                .stream()
                .map(file -> new CheckpointManifest.FileEntry(
                        file.name(),
                        file.length(),
                        file.checksum(),
                        Bytes.utf8("legacy/" + file.name()),
                        Bytes.utf8("version-1"),
                        null))
                .toList();
        return new CheckpointManifest(
                checkpointId,
                uuidBytes(UUID.randomUUID()),
                1,
                null,
                null,
                new CheckpointManifest.CreatedBy(randomBytes(8), randomBytes(8), 1),
                new CheckpointManifest.CreatedAt(
                        900,
                        1_000,
                        "CERTIFIED_HOST_CLOCK",
                        randomBytes(8),
                        1,
                        2,
                        3,
                        Bytes.sha256(Bytes.utf8("kafka-b6-legacy-checkpoint")),
                        0,
                        null),
                shard,
                store.metadata().dbIdentity(),
                store.metadata().storeIncarnationUuid(),
                1,
                store.shardMutationSequence(),
                sourcePosition,
                new byte[32],
                Bytes.sha256(Bytes.utf8("kafka-b6-legacy-checkpoint-semantics")),
                List.of(),
                files);
    }

    private static PreparedCommand command(final ShardId shard, final String name) {
        final ProfileRef destination = new ProfileRef(
                Bytes.utf8("b6-destination-" + name),
                1,
                Bytes.sha256(Bytes.utf8("b6-destination-semantics-" + name)),
                ProfileKind.DESTINATION);
        final RetryPolicyRef retryPolicy = new RetryPolicyRef(
                Bytes.utf8("b6-retry-" + name), 1, Bytes.sha256(Bytes.utf8("b6-retry-semantics-" + name)));
        final long deliverAt = System.currentTimeMillis() + 1_000;
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                destination,
                retryPolicy,
                deliverAt,
                deliverAt + 10_000,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                Bytes.utf8("b6-" + name),
                null,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
        return PreparedCommand.schedule(shard, intent, deliverAt + 20_000);
    }

    private static List<Long> produce(
            final String bootstrap,
            final String clusterId,
            final String topic,
            final Uuid topicId,
            final List<PreparedCommand> commands)
            throws Exception {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ProducerConfig.ACKS_CONFIG, "all");
        configuration.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configuration.put("allow.auto.create.topics", false);
        configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.CLIENT_ID_CONFIG, "nereus-delay-b6-source-smoke");
        final List<Long> producedOffsets = new ArrayList<>();
        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(configuration, new ByteArraySerializer(), new ByteArraySerializer())) {
            final GuardedProducer<byte[], byte[]> guarded = (GuardedProducer<byte[], byte[]>) producer;
            final ProducerResourceGuard guard = new ProducerResourceGuard(clusterId, topic, topicId, 0);
            for (PreparedCommand command : commands) {
                final var metadata = guarded.sendGuarded(
                                new ProducerRecord<>(topic, 0, null, CommandCodec.encodeManagedFrame(command)), guard)
                        .get(10, TimeUnit.SECONDS);
                producedOffsets.add(metadata.recordMetadata().offset());
            }
        }
        return List.copyOf(producedOffsets);
    }

    private static void ensureTopic(final Admin admin, final String topic) throws Exception {
        try {
            describe(admin, topic);
            return;
        } catch (Exception missing) {
            // Create the unique smoke topic below.
        }
        final NewTopic newTopic = new NewTopic(topic, 1, (short) 3);
        newTopic.configs(Map.of("message.timestamp.type", "LogAppendTime"));
        admin.createTopics(List.of(newTopic)).all().get(10, TimeUnit.SECONDS);
        for (int attempt = 0; attempt < 30; attempt++) {
            try {
                describe(admin, topic);
                return;
            } catch (Exception ignored) {
                TimeUnit.MILLISECONDS.sleep(250);
            }
        }
        throw new IllegalStateException("Kafka B6 topic metadata did not converge");
    }

    private static TopicDescription describe(final Admin admin, final String topic) throws Exception {
        return admin.describeTopics(List.of(topic)).allTopicNames().get(10, TimeUnit.SECONDS).get(topic);
    }

    private static void requireCommittedOffset(
            final Admin admin, final String groupId, final String topic, final int partition, final long expected)
            throws Exception {
        final TopicPartition topicPartition = new TopicPartition(topic, partition);
        final OffsetAndMetadata actual = admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get(10, TimeUnit.SECONDS)
                .get(topicPartition);
        if (actual == null || actual.offset() != expected) {
            throw new IllegalStateException("Kafka B6 active group offset mismatch: expected=" + expected);
        }
    }

    private static boolean samePosition(
            final com.nereusstream.delay.protocol.SourcePosition left,
            final com.nereusstream.delay.protocol.SourcePosition right) {
        return left != null && right != null && java.util.Arrays.equals(left.canonicalBytes(), right.canonicalBytes());
    }

    private static byte[] uuidBytes(final UUID value) {
        return ByteBuffer.allocate(16)
                .putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits())
                .array();
    }

    private static byte[] randomBytes(final int length) {
        return java.util.Arrays.copyOf(uuidBytes(UUID.randomUUID()), length);
    }

    private static UUID toUuid(final Uuid value) {
        return new UUID(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }

    private static void deleteTree(final Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}
