package com.nereusstream.delay.transport;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.KafkaReceiptSlotResource;
import com.nereusstream.delay.protocol.TargetKafkaReceiptBinding;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerResourceGuard;
import org.apache.kafka.clients.consumer.GuardedFetchEvidence;
import org.apache.kafka.common.Uuid;

/** Actual protected empty read_committed end-range sample, before signing a Target v6 Admission. */
public final class KafkaClientArtifactTargetReceiptAnchor {
    private KafkaClientArtifactTargetReceiptAnchor() {}

    /** Must acquire before sampling, persist/reopen by digest and retain deletion/compaction/pin protection. */
    public interface Protection {
        byte[] requireProtected(KafkaReceiptSlotResource resource, String physicalTopic, int physicalPartition);

        void requireCurrent(
                byte[] protectionDigest,
                KafkaReceiptSlotResource resource,
                String physicalTopic,
                int physicalPartition,
                byte[] actualSdkJarSha256);
    }

    /**
     * Private construction follows this actual guarded response. Its bytes still require an authenticated durable
     * provider.
     */
    public static final class Observation {
        private final KafkaReceiptSlotResource resource;
        private final String topic;
        private final int partition;
        private final long offset;
        private final byte[] protectionDigest;
        private final byte[] image;

        private Observation(
                KafkaReceiptSlotResource resource,
                String topic,
                int partition,
                long offset,
                byte[] protectionDigest,
                GuardedFetchEvidence response,
                byte[] sdk) {
            this.resource = resource;
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.protectionDigest = Bytes.copy(protectionDigest);
            image = CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, 1);
                CanonicalProtobuf.bytes(out, 2, resource.canonicalBytes());
                CanonicalProtobuf.bytes(out, 3, Bytes.utf8(topic));
                CanonicalProtobuf.uint32(out, 4, partition);
                CanonicalProtobuf.int64(out, 5, offset);
                CanonicalProtobuf.bytes(out, 6, protectionDigest);
                CanonicalProtobuf.bytes(
                        out, 7, Bytes.utf8(KafkaClientArtifactTargetReceiptReader.REQUIRED_K1_SOURCE_COMMIT));
                CanonicalProtobuf.bytes(out, 8, sdk);
                CanonicalProtobuf.uint32(out, 9, response.requestVersion());
                CanonicalProtobuf.uint32Bits(out, 10, response.correlationId());
                CanonicalProtobuf.int64(out, 11, response.brokerNodeId());
                CanonicalProtobuf.int64(out, 12, response.sessionId());
                CanonicalProtobuf.int64(out, 13, response.logStartOffset());
                CanonicalProtobuf.bytes(out, 14, response.fetchResponseBodySha256());
            });
        }

        public long offset() {
            return offset;
        }

        public byte[] canonicalBytes() {
            return Bytes.copy(image);
        }

        public byte[] digest() {
            return Bytes.sha256(Bytes.utf8("nereus-delay-target-receipt-anchor\0"), image);
        }

        public byte[] protectionDigest() {
            return Bytes.copy(protectionDigest);
        }

        public TargetKafkaReceiptBinding binding(String targetPhysicalTopic, int slotsPerShard) {
            return new TargetKafkaReceiptBinding(
                    targetPhysicalTopic, resource, topic, slotsPerShard, partition, offset, digest(), protectionDigest);
        }
    }

    /** One bounded poll; timeout, filtering, unstable transaction or moving end returns no anchor. */
    public static CompletableFuture<Optional<Observation>> sample(
            Map<String, Object> configuration,
            KafkaReceiptSlotResource resource,
            String physicalTopic,
            int physicalPartition,
            Protection protection,
            Duration timeout,
            Executor io) {
        Objects.requireNonNull(resource, "Registry receipt resource");
        Objects.requireNonNull(protection, "durable protection authority");
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative() || timeout.compareTo(Duration.ofSeconds(60)) > 0) {
            throw new IllegalArgumentException("receipt anchor requires a finite poll timeout");
        }
        final var config = new HashMap<>(Objects.requireNonNull(configuration, "consumer configuration"));
        if (config.containsKey(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG)) {
            throw new IllegalArgumentException("receipt anchor cannot transform returned records");
        }
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        config.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 4096);
        config.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 4096);
        config.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
        config.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
        final var future = new CompletableFuture<Optional<Observation>>();
        Objects.requireNonNull(io, "caller bounded I/O executor").execute(() -> {
            try {
                future.complete(sampleNow(
                        config, resource, physicalTopic, physicalPartition, protection, timeout, null, () -> {}));
            } catch (RuntimeException | Error failure) {
                future.completeExceptionally(failure);
                if (failure instanceof Error fatal) {
                    throw fatal;
                }
            }
        });
        return future;
    }

    /** Production sampler uses the existing finite I/O owner and holds its pre-reserved reader through cleanup. */
    public static com.nereusstream.delay.ownership.OwnedCheckpointIo<Optional<Observation>> sampleOwned(
            Map<String, Object> configuration,
            KafkaReceiptSlotResource resource,
            String topic,
            int partition,
            Protection protection,
            Duration timeout,
            java.util.concurrent.ThreadPoolExecutor io,
            com.nereusstream.delay.ownership.TargetDestinationSdkBudget.Client budget) {
        final var config = new HashMap<>(configuration);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        config.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 4096);
        config.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 4096);
        config.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
        config.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
        if (config.containsKey(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG)
                || timeout.isNegative()
                || timeout.isZero()
                || timeout.compareTo(Duration.ofSeconds(60)) > 0) {
            throw new IllegalArgumentException("owned anchor requires finite non-transforming configuration");
        }
        try {
            final var owned = com.nereusstream.delay.ownership.OwnedCheckpointIo.submit(
                    guard -> sampleNow(config, resource, topic, partition, protection, timeout, budget, guard),
                    () -> {},
                    io);
            owned.receipt().whenComplete((value, failure) -> {
                if (failure != null) {
                    try {
                        budget.releaseUnstarted();
                    } catch (IllegalStateException started) {
                        /* native cleanup retains its charge */
                    }
                }
            });
            return owned;
        } catch (RuntimeException | Error rejected) {
            budget.releaseUnstarted();
            throw rejected;
        }
    }

    private static Optional<Observation> sampleNow(
            Map<String, Object> config,
            KafkaReceiptSlotResource resource,
            String physicalTopic,
            int physicalPartition,
            Protection protection,
            Duration timeout,
            com.nereusstream.delay.ownership.TargetDestinationSdkBudget.Client budget,
            Runnable guardCheck) {
        guardCheck.run();
        final var sdk = KafkaClientArtifactTargetReceiptReader.requireSdkIdentity();
        final var pin = protection.requireProtected(resource, physicalTopic, physicalPartition);
        Bytes.requireLength(pin, 32, "durable receipt protection digest");
        protection.requireCurrent(pin, resource, physicalTopic, physicalPartition, sdk);
        final var id = resource.receiptTopicUuid();
        final var guard = new ConsumerResourceGuard(
                resource.authenticatedClusterId(),
                physicalTopic,
                new Uuid(id.getMostSignificantBits(), id.getLeastSignificantBits()),
                physicalPartition);
        Optional<Observation> result = Optional.empty();
        org.apache.kafka.clients.consumer.GuardedConsumer<byte[], byte[]> consumer = null;
        boolean constructing = false;
        try {
            guardCheck.run();
            if (budget != null) {
                budget.beginConstruction();
            }
            constructing = true;
            consumer = KafkaClientArtifactSourceConsumerFactory.create(
                    config, resource.authenticatedClusterId(), physicalTopic, id, physicalPartition, timeout);
            final var partition = guard.topicPartition();
            consumer.assign(List.of(partition));
            // ListOffsets supplies only a candidate; the guarded Fetch must authenticate the same end.
            final long candidate =
                    consumer.endOffsets(List.of(partition), timeout).get(partition);
            consumer.seek(partition, candidate);
            protection.requireCurrent(pin, resource, physicalTopic, physicalPartition, sdk);
            final var records = consumer.pollGuarded(timeout);
            final var response = KafkaClientArtifactFetchEvidence.requireResponse(records, guard);
            final var next = records.nextOffsets().get(partition);
            if (response != null
                    && records.isEmpty()
                    && candidate >= 0
                    && response.fetchOffset() == candidate
                    && response.highWatermark() == candidate
                    && response.lastStableOffset() == candidate
                    && response.firstRecordOffset() == -1
                    && response.lastRecordOffset() == -1
                    && response.logStartOffset() >= 0
                    && response.logStartOffset() <= candidate
                    && next != null
                    && next.offset() == candidate) {
                protection.requireCurrent(pin, resource, physicalTopic, physicalPartition, sdk);
                result = Optional.of(
                        new Observation(resource, physicalTopic, physicalPartition, candidate, pin, response, sdk));
            }
        } catch (RuntimeException | Error failure) {
            if (consumer == null) {
                if (failure instanceof KafkaClientArtifactTargetCleanupFailure held) {
                    throw new KafkaClientArtifactTargetCleanupFailure(held, () -> {
                        held.retryClose();
                        if (budget != null) {
                            budget.closed();
                        }
                    });
                }
                if (budget != null) {
                    if (constructing) {
                        budget.closed();
                    } else {
                        budget.releaseUnstarted();
                    }
                }
            }
            throw failure;
        } finally {
            if (consumer != null) {
                final var actual = consumer;
                try {
                    actual.close(org.apache.kafka.clients.consumer.CloseOptions.timeout(timeout));
                    if (budget != null) {
                        budget.closed();
                    }
                } catch (RuntimeException | Error failure) {
                    throw new KafkaClientArtifactTargetCleanupFailure(failure, () -> {
                        actual.close(org.apache.kafka.clients.consumer.CloseOptions.timeout(timeout));
                        if (budget != null) {
                            budget.closed();
                        }
                    });
                }
            }
        }
        protection.requireCurrent(pin, resource, physicalTopic, physicalPartition, sdk);
        return result;
    }
}
