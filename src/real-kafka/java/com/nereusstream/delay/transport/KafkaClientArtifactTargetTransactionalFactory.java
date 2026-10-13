package com.nereusstream.delay.transport;

import com.nereusstream.delay.protocol.ChannelKind;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import com.nereusstream.delay.protocol.TargetKafkaReceiptBinding;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.kafka.clients.producer.GuardedTransactionalProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** Creates/reopens the exact Target stable transactional ID, without a Lane Journal or private executor. */
public final class KafkaClientArtifactTargetTransactionalFactory {
    private KafkaClientArtifactTargetTransactionalFactory() {}

    public record Limits(Duration operationTimeout, Duration pollTimeout, int maximumRecords, int maximumBytes) {
        public Limits {
            Objects.requireNonNull(operationTimeout, "operation timeout");
            Objects.requireNonNull(pollTimeout, "poll timeout");
            if (operationTimeout.compareTo(Duration.ofSeconds(1)) < 0
                    || operationTimeout.compareTo(Duration.ofSeconds(60)) > 0
                    || pollTimeout.isZero() || pollTimeout.isNegative() || pollTimeout.compareTo(operationTimeout) > 0
                    || maximumRecords <= 0 || maximumBytes <= 0) {
                throw new IllegalArgumentException("Target K2 requires finite SDK/read bounds");
            }
        }
    }

    /** Independent current Owner/CONTROL channel/resource/credential/retention/Artifact authority. */
    @FunctionalInterface
    public interface Activation {
        void requireCurrent(TargetChannelIdentity channel, TargetKafkaReceiptBinding resource, byte[] actualSdkJarSha256);
    }

    public static CompletableFuture<KafkaClientArtifactTargetTransactionalTransport> open(
            Map<String, Object> producerConfiguration, Map<String, Object> readerConfiguration,
            TargetChannelIdentity channel, TargetKafkaReceiptBinding receipt, Activation activation,
            KafkaClientArtifactTargetReceiptReader.Protection protection, Executor io, Limits limits) {
        Objects.requireNonNull(channel, "actual durable channel");
        Objects.requireNonNull(receipt, "receipt context");
        Objects.requireNonNull(activation, "actual activation rights");
        Objects.requireNonNull(protection, "actual mapping/receipt protection");
        Objects.requireNonNull(io, "caller bounded I/O executor");
        Objects.requireNonNull(limits, "limits");
        if (channel.context().kind() != ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT) {
            throw new IllegalArgumentException("K2 factory requires a Target transactional channel");
        }
        final var config = new HashMap<>(Objects.requireNonNull(producerConfiguration, "producer configuration"));
        final var reads = Map.copyOf(Objects.requireNonNull(readerConfiguration, "reader configuration"));
        if (config.containsKey(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG)) {
            throw new IllegalArgumentException("Target K2 cannot run record-transforming interceptors");
        }
        final String stableId = new String(channel.context().producerIdentity(), StandardCharsets.UTF_8);
        if (config.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG)
                && !stableId.equals(config.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG))) {
            throw new IllegalArgumentException("Target transaction configuration changes the durable stable ID");
        }
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, stableId);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        final int millis = Math.toIntExact(limits.operationTimeout().toMillis());
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, (long) millis);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Math.min(millis, 10_000));
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, millis);
        config.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, millis);
        final var opened = new CompletableFuture<KafkaClientArtifactTargetTransactionalTransport>();
        io.execute(() -> {
            Producer<byte[], byte[]> nativeProducer = null;
            try {
                final byte[] actualJar = KafkaClientArtifactTargetReceiptReader.requireSdkIdentity();
                activation.requireCurrent(channel, receipt, actualJar);
                nativeProducer = new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());
                if (!(nativeProducer instanceof GuardedTransactionalProducer<byte[], byte[]> guarded)) {
                    throw new IllegalStateException("Target K2 requires its locked guarded transactional Producer");
                }
                // Actual same-ID SDK initialization resolves/fences the previous Producer epoch.
                // This completion alone does not certify NOT_PUBLISHED or release the durable Source slot.
                activation.requireCurrent(channel, receipt, actualJar);
                guarded.initTransactions();
                activation.requireCurrent(channel, receipt, actualJar);
                final var transport = new KafkaClientArtifactTargetTransactionalTransport(guarded, channel, receipt,
                        io, reads, protection, limits.operationTimeout(), limits.pollTimeout(),
                        limits.maximumRecords(), limits.maximumBytes());
                if (!opened.complete(transport)) { transport.close(); }
            } catch (RuntimeException | Error failure) {
                if (nativeProducer != null) {
                    try { nativeProducer.close(limits.operationTimeout()); }
                    catch (RuntimeException | Error closeFailure) { failure.addSuppressed(closeFailure); }
                }
                opened.completeExceptionally(failure);
                if (failure instanceof Error fatal) { throw fatal; }
            }
        });
        return opened;
    }
}
