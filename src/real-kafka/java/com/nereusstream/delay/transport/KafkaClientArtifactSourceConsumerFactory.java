package com.nereusstream.delay.transport;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerResourceGuard;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Uuid;

/** Creates a Kafka source consumer bound to one immutable resource identity. */
public final class KafkaClientArtifactSourceConsumerFactory {
    private KafkaClientArtifactSourceConsumerFactory() {}

    /**
     * Creates and binds the guarded source consumer. The source binding never
     * accepts a stock Kafka Consumer because it cannot prove Fetch TopicId
     * identity at the Delay source boundary.
     */
    public static GuardedConsumer<byte[], byte[]> create(
            final Map<String, Object> configuration,
            final String authenticatedClusterId,
            final String physicalTopic,
            final UUID nativeTopicUuid,
            final int partition) {
        final Map<String, Object> sourceConfiguration =
                new HashMap<>(Objects.requireNonNull(configuration, "configuration"));
        sourceConfiguration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        final ConsumerResourceGuard guard = new ConsumerResourceGuard(
                authenticatedClusterId, physicalTopic, toKafkaUuid(nativeTopicUuid), partition);
        final Consumer<byte[], byte[]> nativeConsumer = new KafkaConsumer<>(sourceConfiguration);
        try {
            if (!(nativeConsumer instanceof GuardedConsumer<?, ?>)) {
                throw new IllegalStateException("Kafka source requires the guarded K1 Consumer artifact");
            }
            @SuppressWarnings("unchecked")
            final GuardedConsumer<byte[], byte[]> guarded = (GuardedConsumer<byte[], byte[]>) nativeConsumer;
            guarded.bindResourceGuard(guard);
            return guarded;
        } catch (RuntimeException | Error failure) {
            try {
                nativeConsumer.close();
            } catch (RuntimeException | Error closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private static Uuid toKafkaUuid(final UUID uuid) {
        Objects.requireNonNull(uuid, "nativeTopicUuid");
        return new Uuid(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }
}
