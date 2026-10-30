package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.KafkaTargetResource;
import com.nereusstream.delay.adapter.PinnedKafkaDestinationAdapter;
import com.nereusstream.delay.ownership.TargetDestinationActivation;
import com.nereusstream.delay.ownership.TargetDestinationChannelRuntime;
import com.nereusstream.delay.ownership.TargetSdkArtifactIdentity;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.ChannelKind;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import java.time.Duration;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.producer.GuardedProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerResourceGuard;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** Actual bounded K1 baseline opening; uses neither transaction nor a receipt/Journal substitute. */
public final class KafkaClientArtifactTargetBaselineFactory {
    private KafkaClientArtifactTargetBaselineFactory() {}

    public record Configuration(Map<String,String> operational, byte[] sdkJar, String topic,
            Duration timeout, long bufferBytes, int maximumRequestBytes,
            com.nereusstream.delay.protocol.CapacityVector cost) {
        public Configuration {
            com.nereusstream.delay.protocol.Bytes.requireLength(sdkJar, 32, "K1 SDK artifact hash");
            sdkJar = com.nereusstream.delay.protocol.Bytes.copy(sdkJar);
            operational = Map.copyOf(operational); Objects.requireNonNull(cost); Objects.requireNonNull(timeout);
            if (topic == null || !topic.matches("[a-zA-Z0-9._-]{1,249}") || topic.equals(".") || topic.equals("..")
                    || timeout.toMillis() < 1000 || timeout.toMillis() > 60_000 || maximumRequestBytes < 1
                    || maximumRequestBytes == Integer.MAX_VALUE || bufferBytes < maximumRequestBytes
                    || bufferBytes == Long.MAX_VALUE || operational.size() > 64) {
                throw new IllegalArgumentException("configured K1 Target requires finite native settings");
            }
            operational.forEach((key, value) -> {
                if (key.isBlank() || key.length() > 128 || value.isBlank() || value.length() > 4096) {
                    throw new IllegalArgumentException("configured K1 operational settings exceed their literal bound");
                }
            });
            if (cost.amount(com.nereusstream.delay.protocol.CapacityDimension.ADAPTER_PRODUCERS) < 1
                    || cost.amount(com.nereusstream.delay.protocol.CapacityDimension.ADAPTER_THREADS) < 1
                    || cost.amount(com.nereusstream.delay.protocol.CapacityDimension.BUFFERED_MESSAGES) < 1
                    || cost.amount(com.nereusstream.delay.protocol.CapacityDimension.BUFFERED_BYTES) < bufferBytes) {
                throw new IllegalArgumentException("configured K1 cost does not cover its actual producer/buffer");
            }
        }
        @Override public byte[] sdkJar() { return com.nereusstream.delay.protocol.Bytes.copy(sdkJar); }
    }

    public static Configuration load(Path path) throws java.io.IOException {
        final var f = com.nereusstream.delay.ownership.TargetWorkerRuntimeConfigLoader.loadFields(path);
        final var result = new Configuration(f.prefixed("kafka.", 64), f.hex("sdk.sha256", 32, 32),
                f.text("target.topic"), Duration.ofMillis(f.number("producer.timeoutMs")),
                f.number("producer.bufferBytes"), f.integer("producer.maximumRequestBytes"), f.capacity("producer.cost."));
        f.requireComplete(); return result;
    }

    public static TargetDestinationChannelRuntime openConfigured(
            com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup,
            com.nereusstream.delay.ownership.TargetWorkerPublicationDispatch dispatch, byte[] channelDigest, Path configuration,
            com.nereusstream.delay.ownership.LocalBrokerMtlsResolver resolver,
            com.nereusstream.delay.runtime.ProfileCatalog profiles,
            com.nereusstream.delay.runtime.CredentialAttestationTrustSet trust,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            java.util.function.Supplier<com.nereusstream.delay.store.BoundedReadBudget> reads,
            com.nereusstream.delay.ownership.TargetDestinationSdkBudget budget) throws java.io.IOException {
        return openConfigured(startup, dispatch, channelDigest, load(configuration), resolver, profiles, trust, time, reads, budget);
    }

    public static TargetDestinationChannelRuntime openConfigured(
            com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup,
            com.nereusstream.delay.ownership.TargetWorkerPublicationDispatch dispatch, byte[] channelDigest, Configuration config,
            com.nereusstream.delay.ownership.LocalBrokerMtlsResolver resolver,
            com.nereusstream.delay.runtime.ProfileCatalog profiles,
            com.nereusstream.delay.runtime.CredentialAttestationTrustSet trust,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            java.util.function.Supplier<com.nereusstream.delay.store.BoundedReadBudget> reads,
            com.nereusstream.delay.ownership.TargetDestinationSdkBudget budget) {
        Objects.requireNonNull(config);
        try {
            final var activation = startup.preparePublicationChannel(channelDigest, resolver, profiles, trust, reads, time);
            final var factory = opening(new HashMap<>(config.operational()), activation.channel(), activation.physical(),
                    config.topic(), config.timeout(), config.bufferBytes(), config.maximumRequestBytes(), activation, config.sdkJar());
            return startup.openPublicationChannel(dispatch, activation.channel(), activation.physical(), budget, config.cost(),
                    config.maximumRequestBytes(), factory, reads);
        } catch (RuntimeException | Error failure) { startup.failResourceStartup(failure); throw failure; }
    }

    public static TargetDestinationChannelRuntime.Open opening(Map<String,Object> supplied,
            TargetChannelIdentity channel, CanonicalTargetPartition physical, String topic,
            Duration operationTimeout, long bufferBytes, int maximumRequestBytes,
            TargetDestinationActivation activation, byte[] activatedJar) {
        Objects.requireNonNull(activation,"actual Profile/Source activation");
        if(channel.context().kind()!=ChannelKind.BASELINE_PRODUCER
                ||physical.resource().kind()!=com.nereusstream.delay.protocol.BrokerResourceIdentity.Kind.KAFKA
                ||!physical.id().equals(channel.context().target())||bufferBytes<maximumRequestBytes||maximumRequestBytes<=0
                ||operationTimeout.toMillis()<1000||operationTimeout.toMillis()>60000) {
            throw new IllegalArgumentException("K1 baseline requires finite buffers/time and exact Source Target");
        }
        final var receipt = activation.verifiedMaterial();
        final var config=new HashMap<>(KafkaClientArtifactMtlsConfiguration.apply(supplied, receipt));
        if(config.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG)||config.containsKey(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG)) {
            throw new IllegalArgumentException("baseline must not acquire transactions or transform records");
        }
        config.put(ProducerConfig.ACKS_CONFIG,"all"); config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,1); config.put(ProducerConfig.LINGER_MS_CONFIG,0);
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG,bufferBytes); config.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,maximumRequestBytes);
        final int millis=Math.toIntExact(operationTimeout.toMillis());
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG,(long)millis); config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,millis);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,Math.min(millis,10000));
        final var frozen=Map.copyOf(config);
        final byte[] expected=com.nereusstream.delay.protocol.Bytes.copy(activatedJar);
        return new TargetDestinationChannelRuntime.Open() {
            @Override public com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.Receipt credentialReceipt() { return receipt; }
            @Override public void requireLoadedMaterial(TargetChannelIdentity current) { activation.requireLoadedMaterial(current); }
            @Override public com.nereusstream.delay.adapter.DestinationPublishAdapter create(
                    Runnable guard, com.nereusstream.delay.ownership.TargetDestinationSdkBudget.Client reserved) {
                Producer<byte[],byte[]> producer=null; boolean constructing=false;
                try {
                    guard.run();activation.run();TargetSdkArtifactIdentity.requireExact(GuardedProducer.class,expected);
                    reserved.beginConstruction();constructing=true;
                    producer=new KafkaProducer<>(frozen,new ByteArraySerializer(),new ByteArraySerializer());
                    if(!(producer instanceof GuardedProducer<byte[],byte[]> guarded))throw new IllegalStateException("actual K1 guard is required");
                    final var id=physical.resource().kafka().nativeTopicUuid();
                    guarded.requireResourceGuard(new ProducerResourceGuard(physical.resource().kafka().authenticatedClusterId(),topic,
                            new Uuid(id.getMostSignificantBits(),id.getLeastSignificantBits()),Math.toIntExact(physical.physicalPartition())));
                    guard.run();activation.run();
                    final var resource=new KafkaTargetResource(physical.resource().kafka().authenticatedClusterId(),id,
                            Math.toIntExact(physical.physicalPartition()));
                    final var transport=new KafkaClientArtifactTargetDestinationTransport(guarded,resource,topic,channel.context(),
                            operationTimeout,reserved,activation);
                    return new PinnedKafkaDestinationAdapter(resource,transport);
                } catch(RuntimeException|Error failure) {
                    if(producer!=null) {
                        final var actual=producer;
                        try { KafkaClientArtifactProducerCleanup.close(actual, operationTimeout);reserved.closed(); }
                        catch(RuntimeException|Error closeFailure) {
                            final var held=new KafkaClientArtifactTargetCleanupFailure(failure,()->{KafkaClientArtifactProducerCleanup.close(actual, operationTimeout);reserved.closed();});
                            held.addSuppressed(closeFailure);throw held;
                        }
                    } else if (failure instanceof KafkaProducer.ConstructionCleanupFailure partial) {
                        throw KafkaClientArtifactProducerCleanup.partial(partial, operationTimeout, reserved::closed);
                    } else if(constructing && failure instanceof org.apache.kafka.common.config.ConfigException)reserved.closed();
                    else if (!constructing) reserved.releaseUnstarted();
                    else throw new KafkaClientArtifactTargetCleanupFailure(failure, () -> { throw new IllegalStateException("producer construction has no native termination fact"); });
                    throw failure;
                }
            }
        };
    }
}
