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
                        try { actual.close(operationTimeout);reserved.closed(); }
                        catch(RuntimeException|Error closeFailure) {
                            final var held=new KafkaClientArtifactTargetCleanupFailure(failure,()->{actual.close(operationTimeout);reserved.closed();});
                            held.addSuppressed(closeFailure);throw held;
                        }
                    } else if(constructing)reserved.closed();else reserved.releaseUnstarted();
                    throw failure;
                }
            }
        };
    }
}
